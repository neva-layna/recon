package com.reconciliation.synthdata;

import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Parquet OutputFile backed by the local filesystem.
 */
final class LocalOutputFile implements OutputFile {
    private final Path path;

    LocalOutputFile(Path path) {
        this.path = path;
    }

    @Override
    public PositionOutputStream create(long blockSizeHint) throws IOException {
        createParentDirectories();
        return new LocalPositionOutputStream(Files.newOutputStream(path, StandardOpenOption.CREATE_NEW));
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
        createParentDirectories();
        return new LocalPositionOutputStream(Files.newOutputStream(
            path,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.WRITE
        ));
    }

    @Override
    public boolean supportsBlockSize() {
        return false;
    }

    @Override
    public long defaultBlockSize() {
        return 0L;
    }

    private void createParentDirectories() throws IOException {
        Path parent = path.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
    }

    private static final class LocalPositionOutputStream extends PositionOutputStream {
        private final OutputStream delegate;
        private long position;

        private LocalPositionOutputStream(OutputStream delegate) {
            this.delegate = delegate;
        }

        @Override
        public long getPos() {
            return position;
        }

        @Override
        public void write(int value) throws IOException {
            delegate.write(value);
            position++;
        }

        @Override
        public void write(byte[] bytes, int offset, int length) throws IOException {
            delegate.write(bytes, offset, length);
            position += length;
        }

        @Override
        public void flush() throws IOException {
            delegate.flush();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
