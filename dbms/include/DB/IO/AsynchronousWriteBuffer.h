#pragma once

#include <math.h>

#include <vector>

#include <Poco/SharedPtr.h>

#include <statdaemons/threadpool.hpp>

#include <DB/IO/WriteBuffer.h>


namespace DB
{

using Poco::SharedPtr;
	

/** Записывает данные асинхронно с помощью двойной буферизации.
  */
class AsynchronousWriteBuffer : public WriteBuffer
{
private:
	WriteBuffer & out;				/// Основной буфер, отвечает за запись данных.
	std::vector<char> memory;		/// Кусок памяти для дублирования буфера.
	boost::threadpool::pool pool;	/// Для асинхронной записи данных.
	bool started;					/// Была ли запущена асинхронная запись данных.

	/// Менять местами основной и дублирующий буферы.
	void swapBuffers()
	{
		buffer().swap(out.buffer());
		std::swap(position(), out.position());
	}

	void nextImpl()
	{
		if (!offset())
			return;

		if (started)
			pool.wait();
		else
			started = true;

		if (exception)
			exception->rethrow();

		swapBuffers();

		/// Данные будут записываться в отельном потоке.
		pool.schedule(boost::bind(&AsynchronousWriteBuffer::thread, this));
	}

public:
	AsynchronousWriteBuffer(WriteBuffer & out_) : WriteBuffer(NULL, 0), out(out_), memory(out.buffer().size()), pool(1), started(false)
	{
		/// Данные пишутся в дублирующий буфер.
		set(&memory[0], memory.size());
	}

	~AsynchronousWriteBuffer()
	{
		bool uncaught_exception = std::uncaught_exception();

		try
		{
			if (started)
				pool.wait();
			if (exception)
				exception->rethrow();

			swapBuffers();
			out.next();
		}
		catch (...)
		{
			/// Если до этого уже было какое-то исключение, то второе исключение проигнорируем.
			if (!uncaught_exception)
				throw;
		}
	}

	SharedPtr<Exception> exception;

	/// То, что выполняется в отдельном потоке
	void thread()
	{
		try
		{
			out.next();
		}
		catch (const Exception & e)
		{
			exception = new Exception(e);
		}
		catch (const Poco::Exception & e)
		{
			exception = new Exception(e.message(), ErrorCodes::POCO_EXCEPTION);
		}
		catch (const std::exception & e)
		{
			exception = new Exception(e.what(), ErrorCodes::STD_EXCEPTION);
		}
		catch (...)
		{
			exception = new Exception("Unknown exception", ErrorCodes::UNKNOWN_EXCEPTION);
		}
	}
};

}
