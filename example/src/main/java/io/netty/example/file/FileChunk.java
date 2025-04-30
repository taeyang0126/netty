package io.netty.example.file;

/**
 * <p>
 * FileChunk
 * </p>
 *
 * @author 伍磊
 */
public class FileChunk {

    private byte[] bytes;

    private long position;

    private boolean lastChunk;

    public byte[] getBytes() {
        return bytes;
    }

    public void setBytes(byte[] bytes) {
        this.bytes = bytes;
    }

    public long getPosition() {
        return position;
    }

    public void setPosition(long position) {
        this.position = position;
    }

    public boolean isLastChunk() {
        return lastChunk;
    }

    public void setLastChunk(boolean lastChunk) {
        this.lastChunk = lastChunk;
    }
}
