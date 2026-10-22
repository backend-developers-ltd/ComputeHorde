HTTP PUT output uploads send the file in chunks of at most 64 KiB instead of one chunk per line, so a file without newlines is no longer read into memory whole.
