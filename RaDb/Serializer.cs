using ProtoBuf.Meta;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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

        public byte[] Serialize(T value)
        {
            if (null == value) return emptyBuffer; 

            using (var memoryStream = new MemoryStream())
            {
                meta.Serialize(memoryStream, value);
                memoryStream.Position = 0;
                var buffer = new byte[memoryStream.Length];
                memoryStream.Read(buffer, 0, buffer.Length);
                return buffer;
            }
        }

        public T Deserialize(Stream stream, int length)
        {
            var t = default(T);
            if (length == 0) return t;
            return (T)meta.Deserialize(stream, t, typeof(T), length);
        }
    }
}
