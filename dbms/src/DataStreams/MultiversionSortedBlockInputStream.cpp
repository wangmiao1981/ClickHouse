#include <Common/FieldVisitors.h>
#include <DataStreams/MultiversionSortedBlockInputStream.h>
#include <Columns/ColumnsNumber.h>

namespace DB
{

namespace ErrorCodes
{
extern const int INCORRECT_DATA;
extern const int LOGICAL_ERROR;
}

void MultiversionSortedBlockInputStream::insertGap(size_t gap_size, bool add_row)
{
    if (out_row_sources_buf)
    {
        current_row_sources.resize(gap_size + (add_row ? 1 : 0));
        for (size_t i = 0; i < gap_size; ++i)
            current_row_sources[i].setSkipFlag(true);

        if (add_row)
            current_row_sources[gap_size].setSkipFlag(false);

        out_row_sources_buf->write(
                reinterpret_cast<const char *>(current_row_sources.data()),
                current_row_sources.size() * sizeof(RowSourcePart));
    }
}

void MultiversionSortedBlockInputStream::insertRow(size_t skip_rows, const RowRef & row, MutableColumns & merged_columns)
{
    const auto & columns = row.shared_block->all_columns;
    for (size_t i = 0; i < num_columns; ++i)
        merged_columns[i]->insertFrom(*columns[i], row.row_num);

    insertGap(skip_rows, true);
}


Block MultiversionSortedBlockInputStream::readImpl()
{
    if (finished)
        return {};

    if (children.size() == 1)
        return children[0]->read();

    Block header;
    MutableColumns merged_columns;

    bool is_initialized = !first;

    init(header, merged_columns);

    if (has_collation)
        throw Exception("Logical error: " + getName() + " does not support collations", ErrorCodes::LOGICAL_ERROR);

    if (merged_columns.empty())
        return {};

    /// Additional initialization.
    if (!is_initialized)
        sign_column_number = header.getPositionByName(sign_column);


    merge(merged_columns, queue);
    return header.cloneWithColumns(std::move(merged_columns));
}


void MultiversionSortedBlockInputStream::merge(MutableColumns & merged_columns, std::priority_queue<SortCursor> & queue)
{
    size_t merged_rows = 0;

    auto update_queue = [this, & queue](SortCursor & cursor)
    {
        if (!cursor->isLast())
        {
            cursor->next();
            queue.push(cursor);
        }
        else
        {
            /// We take next block from the corresponding source, if there is one.
            fetchNextBlock(cursor, queue);
        }
    };

    /// Take rows in correct order and put them into `merged_columns` until the rows no more than `max_block_size`
    while (!queue.empty())
    {
        SortCursor current = queue.top();

        RowRef next_key;

        Int8 sign = static_cast<const ColumnInt8 &>(*current.impl->shared_block->all_columns[sign_column_number])
                .getData()[current->pos];

        setRowRef(next_key, current);


        size_t rows_to_merge = 0;

        if (current_keys.empty())
        {
            sign_in_queue = sign;
            current_keys.pushBack(next_key);
            update_queue(current);
        }
        else
        {
            if (current_keys.back() == next_key)
            {
                update_queue(current);

                /// If all the rows was collapsed, we still want to give at least one block in the result.
                /// If queue is empty then don't collapse two last rows.
                if (sign == sign_in_queue || (blocks_written == 0 && merged_rows == 0 && queue.empty() && current_keys.size() == 1))
                    current_keys.pushBack(next_key);
                else
                {
                    current_keys.popBack();
                    current_keys.pushGap();
                }
            }
            else
                rows_to_merge = current_keys.size();
        }

        if (current_keys.size() > max_rows_in_queue)
            rows_to_merge = current_keys.size() - max_rows_in_queue;

        while (rows_to_merge)
        {
            if (merged_rows >= max_block_size)
            {
                ++blocks_written;
                return;
            }

            const auto & row = current_keys.front();
            auto gap = current_keys.frontGap();

            insertRow(gap, row, merged_columns);

            current_keys.popFront();

            ++merged_rows;
            --rows_to_merge;
        }
    }

    while (!current_keys.empty())
    {
        const auto & row = current_keys.front();
        auto gap = current_keys.frontGap();

        insertRow(gap, row, merged_columns);

        current_keys.popFront();
        ++merged_rows;
    }

    insertGap(current_keys.frontGap(), false);

    finished = true;
}

}
