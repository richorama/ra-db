using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;

namespace RaDb
{
    public static class ExtensionMethods
    {

        public static byte[] GetBytes(this object value)
        {
            if (value == null) return new byte[0];

            var bf = new BinaryFormatter();
            using (var ms = new MemoryStream())
            {
                bf.Serialize(ms, value);
                return ms.ToArray();
            }
        }

        public static T GetObject<T>(this byte[] value)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));

            if (value.Length == 0) return default(T);

            var bf = new BinaryFormatter();
            using (var memStream = new MemoryStream())
            {
                memStream.Write(value, 0, value.Length);
                memStream.Seek(0, SeekOrigin.Begin);
                return (T)bf.Deserialize(memStream);
            }
        }

        public static T ReadObject<T>(this Stream stream, int length)
        {
            if (length == 0) return default(T);

            var buffer = new byte[length];
            stream.Read(buffer, 0, length);
            return buffer.GetObject<T>();
        }

        public static byte[] GetBytes(this string str)
        {
            //var bytes = new byte[str.Length * sizeof(char)];
            //Buffer.BlockCopy(str.ToCharArray(), 0, bytes, 0, bytes.Length);
            return Encoding.Default.GetBytes(str);
            //return bytes;
        }

        public static string GetString(this byte[] bytes)
        {
            return Encoding.Default.GetString(bytes);
            /*
            var chars = new char[bytes.Length / sizeof(char)];
            Buffer.BlockCopy(bytes, 0, chars, 0, bytes.Length);
            return new string(chars);*/
        }

        public static int ReadInt(this Stream stream)
        {
            var buffer = new byte[sizeof(int)];
            stream.Read(buffer, 0, sizeof(int));
            return BitConverter.ToInt32(buffer,0);
        }

        public static string ReadString(this Stream stream, int length)
        {
            var buffer = new byte[length];
            stream.Read(buffer, 0, length);
            return buffer.GetString();
        }

        public static byte[] GetBuffer<T>(this LogEntry<T> entry)
        {
            var keyBuffer = entry.Key.GetBytes();
            var valueBuffer = entry.Value.GetBytes();

            var buffer = new byte[keyBuffer.Length + valueBuffer.Length + 4 + 4 + 1];
            var index = 0;
            var append = new Action<byte[]>(bytes =>
            {
                Buffer.BlockCopy(bytes, 0, buffer, index, bytes.Length);
                index += bytes.Length;
            });

            append(BitConverter.GetBytes(keyBuffer.Length));
            append(BitConverter.GetBytes(valueBuffer.Length));
            append(new byte[] { (byte)entry.Operation });
            append(keyBuffer);
            append(valueBuffer);
            return buffer;
        }

        public static LogEntry<T> ReadEntry<T>(this Stream stream)
        {
            var keySize = stream.ReadInt();
            var valueSize = stream.ReadInt();
            var operation = (Operation)stream.ReadByte();
            return new LogEntry<T>
            {
                Key = stream.ReadString(keySize),
                Value = stream.ReadObject<T>(valueSize),
                Operation = operation
            };
        }

        public static IEnumerable<LogEntry<T>> ReadAll<T>(this Stream stream)
        {
            stream.Position = 0;
            while (stream.Position < stream.Length)
            {
                yield return stream.ReadEntry<T>();
            }
        }

        public static void ApplyOperation<T>(this IDictionary<string, T> dictionary, LogEntry<T> value)
        {
            switch (value.Operation)
            {
                case Operation.Write:
                    if (dictionary.ContainsKey(value.Key))
                    {
                        dictionary[value.Key] = value.Value;
                    }
                    else
                    {
                        dictionary.Add(value.Key, value.Value);
                    }
                    break;
                case Operation.Delete:
                    if (dictionary.ContainsKey(value.Key))
                    {
                        dictionary.Remove(value.Key);
                    }
                    break;
            }

        }

    }
}
