using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading;

namespace RaDb
{
    public delegate void LogEntryHandler(LogEntry logEvent);

    public class Log : IDisposable
    {
        Stream logStream;
        ConcurrentDictionary<string, string> cache = new ConcurrentDictionary<string, string>();
        public event LogEntryHandler LogEvent;
        HashSet<string> deletedKeys = new HashSet<string>();

        public Log(string filename)
        {
            if (null == filename) throw new ArgumentNullException(nameof(filename));

            this.logStream = new FileStream(filename, FileMode.OpenOrCreate);
            LoadCache();
        }

        public Log(Stream stream)
        {
            if (null == stream) throw new ArgumentNullException(nameof(stream));

            this.logStream = stream;
            LoadCache();
        }

        void LoadCache()
        {
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
                    cache.AddOrUpdate(entry.Key, x => entry.Value, (z,y) => entry.Value);
                    deletedKeys.Remove(entry.Key);
                    break;
                case Operation.Delete:
                    string _ = null;
                    cache.TryRemove(entry.Key, out _);
                    deletedKeys.Add(entry.Key);
                    break;
            }
        }

        public IEnumerable<string> Keys
        {
            get
            {
                return this.cache.Keys;
            }
        }

        public IEnumerable<string> DeletedKeys
        {
            get
            {
                return this.deletedKeys;
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
            if (null == key) throw new ArgumentNullException(nameof(key));

            if (cache.ContainsKey(key))
            {
                return cache[key];
            }
            return null;
        }

        public void Del(string key, bool requireFlush = false)
        {
            if (null == key) throw new ArgumentNullException(nameof(key));

            var entry = LogEntry.CreateDelete(key);
            this.Append(entry, requireFlush);
            this.ApplyToCache(entry);
            if (null != this.LogEvent) this.LogEvent(entry);
        }

       

        public void Set(string key, string value, bool requireFlush = false)
        {
            if (null == key) throw new ArgumentNullException(nameof(key));
            if (null == value) throw new ArgumentNullException(nameof(value));

            var entry = LogEntry.CreateWrite(key, value);
            this.Append(entry, requireFlush);
            ApplyToCache(entry);
            if (null != this.LogEvent) this.LogEvent(entry);
        }

      

        void Append(LogEntry entry, bool requireFlush)
        {
            var buffer = GetBuffer(entry);
            try
            {
                Monitor.Enter(logStream);
                logStream.Write(buffer, 0, buffer.Length);
                if (requireFlush) logStream.Flush();
            }
            finally
            {
                Monitor.Exit(logStream);
            }
        }

     

        static byte[] GetBuffer(LogEntry entry)
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
            logStream.Dispose();
        }
    }
}
