using System.Diagnostics.Contracts;
using System.IO;
namespace com.alicloud.openservices.tablestore.core.protocol
{
    public class PlainBufferOutputStream
    {
        private readonly byte[] buffer;
        private readonly int capacity;
        private int pos;

        public PlainBufferOutputStream(int capacity)
        {
            Contract.Requires(capacity > 0);
            buffer = new byte[capacity];
            this.capacity = capacity;
        }

        public byte[] GetBuffer()
        {
            return buffer;
        }

        public bool IsFull()
        {
            return pos == capacity;
        }

        public int Count()
        {
            return pos;
        }

        public int Remain()
        {
            return capacity - pos;
        }

        public void Clear()
        {
            this.pos = 0;
        }

        public void WriteRawByte(byte value)
        {
            if (pos == capacity)
            {
                throw new IOException("The buffer is full.");
            }

            buffer[pos++] = value;
        }

        public void WriteRawByte(int value)
        {
            WriteRawByte((byte)value);
        }

        /// <summary>
        /// Write a little-endian 32-bit integer.
        /// </summary>
        /// <param name="value">Value.</param>
        public void WriteRawLittleEndian32(int value)
        {
            WriteRawByte((value) & 0xFF);
            WriteRawByte((value >> 8) & 0xFF);
            WriteRawByte((value >> 16) & 0xFF);
            WriteRawByte((value >> 24) & 0xFF);
        }

        /// <summary>
        /// Write a little-endian 64-bit integer.
        /// </summary>
        /// <param name="value">Value.</param>
        public void WriteRawLittleEndian64(long value)
        {
            WriteRawByte((int)(value) & 0xFF);
            WriteRawByte((int)(value >> 8) & 0xFF);
            WriteRawByte((int)(value >> 16) & 0xFF);
            WriteRawByte((int)(value >> 24) & 0xFF);
            WriteRawByte((int)(value >> 32) & 0xFF);
            WriteRawByte((int)(value >> 40) & 0xFF);
            WriteRawByte((int)(value >> 48) & 0xFF);
            WriteRawByte((int)(value >> 56) & 0xFF);
        }

        public void WriteDouble(double value)
        {
            WriteRawLittleEndian64(System.BitConverter.DoubleToInt64Bits(value));
        }

        public void WriteBool(bool value)
        {
            WriteRawByte(value ? 1 : 0);
        }

        public void WriteBytes(byte[] bytes)
        {
            if (this.pos + bytes.Length > this.capacity)
            {
                throw new IOException("The buffer is full.");
            }

            System.Array.Copy(bytes, 0, this.buffer, this.pos, bytes.Length);
            this.pos += bytes.Length;
        }

        public const int LITTLE_ENDIAN_32_SIZE = 4;
        public const int LITTLE_ENDIAN_64_SIZE = 8;
    }
}
