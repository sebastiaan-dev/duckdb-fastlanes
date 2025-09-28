#include "write_fls.hpp"

#include <duckdb/function/copy_function.hpp>
#include <duckdb/main/extension_util.hpp>
#include "fls/common/magic_enum.hpp"
#include "writer/fls_writer.hpp"

namespace duckdb {

void WriteFastLanes::Register(DatabaseInstance &db) {
	CopyFunction fn("fls");

	fn.copy_to_bind = FastLanesFileWriter::Bind;
	fn.copy_to_initialize_local = FastLanesFileWriter::InitLocal;
	fn.copy_to_initialize_global = FastLanesFileWriter::InitGlobal;
	fn.copy_to_sink = FastLanesFileWriter::Sink;
	fn.copy_to_combine = FastLanesFileWriter::Combine;
	fn.copy_to_finalize = FastLanesFileWriter::Finalize;
	fn.execution_mode = FastLanesFileWriter::GetExecutionMode;

	fn.prepare_batch = FastLanesFileWriter::PrepareBatch;
	fn.flush_batch = FastLanesFileWriter::FlushBatch;
	fn.desired_batch_size = FastLanesFileWriter::GetDesiredBatchsize;

	fn.rotate_files = FastLanesFileWriter::RotateFiles;
	fn.rotate_next_file = FastLanesFileWriter::RotateNextFile;

	fn.extension = "fls";

	ExtensionUtil::RegisterFunction(db, fn);
}

} // namespace duckdb