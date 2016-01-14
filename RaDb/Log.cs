using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace RaDb
{
    public delegate void LogEntryHandler(LogEntry logEvent);

    public class Log : IDisposable
    {
        FileStream logStream;
        IDictionary<string,string> cache;

        public event LogEntryHandler LogEvent;

        public Log(string filename)
        {
            this.logStream = new FileStream(filename, FileMode.OpenOrCreate);
            cache = new Dictionary<string, string>();
            foreach (var entry in this.ReadAll())
            {
                ApplyToCache(entry);
            }
        }

        void ApplyToCache(LogEntry entry)
        {
            switch (entry.Operation)
            {
                case Operation.Write:
                    if (cache.ContainsKey(entry.Key))
                    {
                        cache[entry.Key] = entry.Value;
                        return;
                    }
                    cache.Add(entry.Key, entry.Value);
                    return;
                case Operation.Delete:
                    if (!cache.ContainsKey(entry.Key)) return;
                    cache.Remove(entry.Key);
                    return;
            }
        }

        public IEnumerable<string> Keys
        {
            get
            {
                return this.cache.Keys;
            }
        }

        public long Size
        {
            get
            {
                return this.logStream.Length;
            }
        }

        public string Get(string key)
        {
            if (cache.ContainsKey(key))
            {
                return cache[key];
            }
            return null;
        }

        public void Del(string key)
        {
            var entry = LogEntry.CreateDelete(key);
            this.Append(entry);
            this.ApplyToCache(entry);
            if (null != this.LogEvent) this.LogEvent(entry);
        }

        public async Task DelAsync(string key)
        {
            var entry = LogEntry.CreateDelete(key);
            await this.AppendAsync(entry);
            this.ApplyToCache(entry);
            if (null != this.LogEvent) this.LogEvent(entry);
        }

        public void Set(string key, string value)
        {
            var entry = LogEntry.CreateWrite(key, value);
            this.Append(entry);
            ApplyToCache(entry);
            if (null != this.LogEvent) this.LogEvent(entry);
        }

        public async Task SetAsync(string key, string value)
        {
            var entry = LogEntry.CreateWrite(key, value);
            await this.AppendAsync(entry);
            ApplyToCache(entry);
            if (null != this.LogEvent) this.LogEvent(entry);
        }

        void Append(LogEntry entry)
        {
            var buffer = GetBuffer(entry);
            logStream.Write(buffer, 0, buffer.Length);
        }

        Task AppendAsync(LogEntry entry)
        {
            var buffer = GetBuffer(entry);
            return logStream.WriteAsync(buffer, 0, buffer.Length);
        }

        private static byte[] GetBuffer(LogEntry entry)
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


        IEnumerable<LogEntry> ReadAll()
        {
            logStream.Position = 0;
            while (logStream.Position < logStream.Length)
            {
                yield return ReadEntry();
            }
        }

        LogEntry ReadEntry()
        {
            var keySize = logStream.ReadInt();
            var valueSize = logStream.ReadInt();
            var operation = (Operation) logStream.ReadByte();
            return new LogEntry
            {
                Key = logStream.ReadString(keySize),
                Value = logStream.ReadString(valueSize),
                Operation = operation
            };
        }

        public void Dispose()
        {
            logStream.Close();
        }
    }
}
