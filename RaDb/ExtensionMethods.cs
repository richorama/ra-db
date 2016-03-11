using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
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

        public static string GetString(this byte[] bytes, int length)
        {
            return Encoding.Default.GetString(bytes, 0, length);
        }

        static byte[] intBuffer = new byte[sizeof(int)];

        public static int ReadInt(this Stream stream)
        {
            stream.Read(intBuffer, 0, intBuffer.Length);
            return BitConverter.ToInt32(intBuffer, 0);
        }

        static byte[] buffer = new byte[4 * 1024];

        public static string ReadString(this Stream stream, int length)
        {
            // grow the buffer
            if (length > buffer.Length) buffer = new byte[length];
            stream.Read(buffer, 0, length);
            return buffer.GetString(length);
        }

        static byte[] tempBuffer = new byte[4 * 1024];

        public static byte[] GetBuffer<T>(this LogEntry<T> entry, ISerializer<T> serializer, out long bufferLength)
        {
            var keyBuffer = entry.Key.GetBytes();
            int valueLength;
            var valueBuffer = serializer.Serialize(entry.Value, out valueLength);

            bufferLength = keyBuffer.Length + valueLength + 4 + 4 + 1;

            // grow the buffer
            if (bufferLength > tempBuffer.Length) tempBuffer = new byte[bufferLength];

            var index = 0;
            var append = new Action<byte[],int>((bytes, length) =>
            {
                if (length == -1) length = bytes.Length;
                Buffer.BlockCopy(bytes, 0, tempBuffer, index, length);
                index += length;
            });

            append(BitConverter.GetBytes(keyBuffer.Length), -1);
            append(BitConverter.GetBytes(valueLength), -1);
            append(new byte[] { (byte)entry.Operation }, -1);
            append(keyBuffer, -1);
            append(valueBuffer, valueLength);
            return tempBuffer;
        }

        public static byte[] GetBuffer<T>(this IEnumerable<LogEntry<T>> entries, ISerializer<T> serializer)
        {
            var output = new List<byte>();
            foreach (var entry in entries)
            {
                long length;
                var buffer = entry.GetBuffer(serializer, out length);
                output.AddRange(buffer.Take((int)length));
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
