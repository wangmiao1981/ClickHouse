#pragma once

#include <common/logger_useful.h>

#include <DataStreams/MergingSortedBlockInputStream.h>

#include <deque>

namespace DB
{

static const size_t MAX_ROWS_IN_MULTIVERSION_QUEUE = 8192;

template <typename T>
class FixedSizeDequeWithGaps
{
public:

    struct ValueWithGap
    {
        size_t gap;
        T value;

        ValueWithGap() : gap(0) {}
        explicit ValueWithGap(const T & value) : value(value), gap(0) {}
    };

    explicit FixedSizeDequeWithGaps(size_t size)
    {
        container.resize(size + 1);
    }

    void pushBack(const T & value)
    {
        container[end] = ValueWithGap(value);
        moveRight(end);
        container[end].gap = 0;
    }

    void pushGap(size_t count = 1) { container[end].gap += count; }

    void popBack()
    {
        size_t curr_gap = container[end].gap;
        moveLeft(end);
        container[end].gap += curr_gap;
    }

    void popFront() { moveRight(begin); }

    T & front() { return container[begin].value; }
    const T & front() const { return container[begin].value; }

    const T & back() const
    {
        size_t ps = end;
        moveLeft(ps);
        return container[ps].value;
    }

    size_t & frontGap() { return container[begin].gap; }
    const size_t & frontGap() const { return container[begin].gap; }

    size_t size() const
    {
        if (begin <= end)
            return end - begin;
        return end + (container.size() - begin);
    }

    bool empty() const { return begin == end; }

private:
    std::vector<ValueWithGap> container;

    size_t gap_before_first = 0;
    size_t begin = 0;
    size_t end = 0;

    void moveRight(size_t & index) const
    {
        ++index;

        if (index == container.size())
            index = 0;
    }

    void moveLeft(size_t & index) const
    {
        if (index == 0)
            index = container.size();

        --index;
    }
};

class MultiversionSortedBlockInputStream : public MergingSortedBlockInputStream
{
public:
    /// Don't need version column. It's in primary key.
    MultiversionSortedBlockInputStream(
            BlockInputStreams inputs_, const SortDescription & description_,
            const String & sign_column_, size_t max_block_size_,
            WriteBuffer * out_row_sources_buf_ = nullptr)
            : MergingSortedBlockInputStream(inputs_, description_, max_block_size_, 0, out_row_sources_buf_)
            , sign_column(sign_column_),
              max_rows_in_queue(std::min(max_block_size_, MAX_ROWS_IN_MULTIVERSION_QUEUE)), current_keys(max_rows_in_queue)
    {
    }

    String getName() const override { return "CollapsingSorted"; }

    String getID() const override
    {
        std::stringstream res;
        res << "MultiversionSortedBlockInputStream(inputs";

        for (const auto & child : children)
            res << ", " << child->getID();

        res << ", description";

        for (const auto & descr : description)
            res << ", " << descr.getID();

        res << ", sign_column, " << sign_column;
        res << ", version_column, " << sign_column << ")";
        return res.str();
    }

protected:
    /// Can return 1 more records than max_block_size.
    Block readImpl() override;

private:
    String sign_column;

    size_t sign_column_number = 0;

    Logger * log = &Logger::get("MultiversionSortedBlockInputStream");

    /// Read is finished.
    bool finished = false;

    Int8 sign_in_queue = 0;
    const size_t max_rows_in_queue;
    FixedSizeDequeWithGaps<RowRef> current_keys; /// Rows with current primary key and same sign.

    size_t blocks_written = 0;

    /// Field specific for VERTICAL merge algorithm.
    PODArray<RowSourcePart> current_row_sources;   /// Sources of rows with the current primary key

    /** We support two different cursors - with Collation and without.
     *  Templates are used instead of polymorphic SortCursors and calls to virtual functions.
     */
    void merge(MutableColumns & merged_columns, std::priority_queue<SortCursor> & queue);

    /// Output to result rows for the current primary key.
    void insertRow(size_t skip_rows, const RowRef & row, MutableColumns & merged_columns);

    void insertGap(size_t gap_size, bool add_row);
};

}
