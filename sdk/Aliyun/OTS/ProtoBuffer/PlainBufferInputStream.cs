using System;
using System.IO;
using Aliyun.OTS.Util;

namespace com.alicloud.openservices.tablestore.core.protocol
{
    public class PlainBufferInputStream
    {
        private readonly MemoryStream buffer;
        private int lastTag;

        public PlainBufferInputStream(MemoryStream buffer)
        {
            this.buffer = buffer;
            this.lastTag = 0;
        }

        public PlainBufferInputStream(byte[] byteBuffer)
        {
            var ms = new MemoryStream();
            ms.SetLength(byteBuffer.Length);
            ms.Write(byteBuffer, 0, byteBuffer.Length);
            this.buffer = ms;
            this.lastTag = 0;
        }

        public bool IsAtEnd()
        {
            return buffer.Position == buffer.Length;
        }

        public int ReadTag()
        {
            if (IsAtEnd())
            {
                lastTag = 0;
                return 0;
            }

            lastTag = ReadRawByte();
            return lastTag;
        }

        public bool CheckLastTagWas(int tag)
        {
            return lastTag == tag;
        }

        public int GetLastTag()
        {
            return lastTag;
        }

        public byte ReadRawByte()
        {
            if (IsAtEnd())
            {
                throw new IOException("Read raw byte encountered EOF.");
            }

            return (byte)buffer.ReadByte();
        }

        public long ReadRawLittleEndian64()
        {
            byte b1 = ReadRawByte();
            byte b2 = ReadRawByte();
            byte b3 = ReadRawByte();
            byte b4 = ReadRawByte();
            byte b5 = ReadRawByte();
            byte b6 = ReadRawByte();
            byte b7 = ReadRawByte();
            byte b8 = ReadRawByte();
            return (((long)b1 & 0xff)) |
                    (((long)b2 & 0xff) << 8) |
                    (((long)b3 & 0xff) << 16) |
                    (((long)b4 & 0xff) << 24) |
                    (((long)b5 & 0xff) << 32) |
                    (((long)b6 & 0xff) << 40) |
                    (((long)b7 & 0xff) << 48) |
                    (((long)b8 & 0xff) << 56);
        }

        public int ReadRawLittleEndian32()
        {
            byte b1 = ReadRawByte();
            byte b2 = ReadRawByte();
            byte b3 = ReadRawByte();
            byte b4 = ReadRawByte();
            return (((int)b1 & 0xff)) |
                    (((int)b2 & 0xff) << 8) |
                    (((int)b3 & 0xff) << 16) |
                    (((int)b4 & 0xff) << 24);
        }

        public bool ReadBoolean()
        {
            return ReadRawByte() != 0;
        }

        public double ReadDouble()
        {
            return BitConverter.Int64BitsToDouble(ReadRawLittleEndian64());
        }

        public int ReadUInt32()
        {
            return ReadRawLittleEndian32();
        }

        public long ReadInt64()
        {
            return ReadRawLittleEndian64();
        }

        public byte[] ReadBytes(int size)
        {
            if (buffer.Length - buffer.Position < size)
            {
                throw new IOException("Read bytes encountered EOF.");
            }

            byte[] result = new byte[size];
            buffer.Read(result, 0, size);
            return result;
        }

        public string ReadUTFString(int size)
        {
            return OtsUtils.Bytes2UTF8String(ReadBytes(size));
        }

        public override string ToString()
        {
            return OtsUtils.Bytes2UTF8String(this.buffer.GetBuffer());
        }
    }
}
