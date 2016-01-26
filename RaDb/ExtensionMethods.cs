using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;

namespace RaDb
{
    internal static class ExtensionMethods
    {
        public static T ReadObject<T>(this Stream stream, ISerializer<T> serializer, int length)
        {
            return serializer.Deserialize(stream, length);
        }

        public static byte[] GetBytes(this string str)
        {
            return Encoding.Default.GetBytes(str);
        }

        public static string GetString(this byte[] bytes)
        {
            return Encoding.Default.GetString(bytes);
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

        public static byte[] GetBuffer<T>(this LogEntry<T> entry, ISerializer<T> serializer)
        {
            var keyBuffer = entry.Key.GetBytes();
            var valueBuffer = serializer.Serialize(entry.Value);

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

        public static byte[] GetBuffer<T>(this IEnumerable<LogEntry<T>> entries, ISerializer<T> serializer)
        {
            var output = new List<byte>();
            foreach (var entry in entries)
            {
                output.AddRange(entry.GetBuffer(serializer));
            }
            return output.ToArray();
        }

        public static LogEntry<T> ReadEntry<T>(this Stream stream, ISerializer<T> serializer)
        {
            var keySize = stream.ReadInt();
            var valueSize = stream.ReadInt();
            var operation = (Operation)stream.ReadByte();
            return new LogEntry<T>
            {
                Key = stream.ReadString(keySize),
                Value = valueSize == 0 ? default(T) : stream.ReadObject<T>(serializer, valueSize),
                Operation = operation
            };
        }

        public static IEnumerable<LogEntry<T>> ReadAll<T>(this Stream stream, ISerializer<T> serializer)
        {
            stream.Position = 0;
            while (stream.Position < stream.Length)
            {
                yield return stream.ReadEntry<T>(serializer);
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
