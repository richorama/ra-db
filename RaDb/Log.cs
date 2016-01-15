using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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

        public void Clear()
        {
            this.logStream.Position = 0;
            this.logStream.SetLength(0);
            this.logStream.Flush();
            this.cache.Clear();
            this.deletedKeys.Clear();
        }

        void LoadCache()
        {
            foreach (var entry in this.logStream.ReadAll())
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

        public DeletableValue GetValueOrDeleted(string key)
        {
            if (null == key) throw new ArgumentNullException(nameof(key));

            if (cache.ContainsKey(key))
            {
                return DeletableValue.FromValue(cache[key]);
            }

            if (deletedKeys.Contains(key))
            {
                return DeletableValue.FromDelete();
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

        internal IEnumerable<LogEntry> Scan(string fromKey, string toKey)
        {
            foreach (var key in this.deletedKeys.Where(x => string.Compare(x, fromKey) >= 0).Where(x => string.Compare(x, toKey) < 0))
            {
                yield return LogEntry.CreateDelete(key);
            }

            foreach (var entry in this.cache.Where(x => string.Compare(x.Key, fromKey) >= 0).Where(x => string.Compare(x.Key, toKey) < 0))
            {
                yield return LogEntry.CreateWrite(entry.Key, entry.Value);
            }
        }

        void Append(LogEntry entry, bool requireFlush)
        {
            var buffer = entry.GetBuffer();
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


        public void Dispose()
        {
            logStream.Dispose();
        }
    }
}
