using ProtoBuf.Meta;
using System;
using System.IO;

namespace RaDb
{
    public class Serializer<T> : ISerializer<T>
    {
        RuntimeTypeModel meta;
        public Serializer()
        {
            meta = TypeModel.Create();
            meta.Add(typeof(T), false).Add(Array.ConvertAll(typeof(T).GetProperties(), prop => prop.Name));
            meta.Compile();
        }

        byte[] emptyBuffer = new byte[0];

        byte[] buffer = new byte[4 * 1024];
        MemoryStream stream = new MemoryStream(4 * 1024);

        public byte[] Serialize(T value, out int length)
        {
            if (null == value)
            {
                length = 0;
                return emptyBuffer;
            }

            stream.Position = 0;
            stream.SetLength(0);

            meta.Serialize(stream, value);

            length = (int)stream.Length;
            stream.Position = 0;

            if (length > buffer.Length)
            {
                // grow the buffer
                buffer = new byte[length];
            }

            stream.Read(buffer, 0, length);
            return buffer;
        }

        public T Deserialize(Stream stream, int length)
        {
            var t = default(T);
            if (length == 0) return t;
            return (T)meta.Deserialize(stream, t, typeof(T), length);
        }
    }
}
