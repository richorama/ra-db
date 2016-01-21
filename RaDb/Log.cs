using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;

namespace RaDb
{
    public delegate void LogEntryHandler<T>(LogEntry<T> logEvent);


    /// <summary>
    /// The log of database operations
    /// </summary>
    public class Log<T> : IDisposable
    {
        Stream logStream;
        ConcurrentDictionary<string, T> cache = new ConcurrentDictionary<string, T>();
        public event LogEntryHandler<T> LogEvent;
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
            foreach (var entry in this.logStream.ReadAll<T>())
            {
                ApplyToCache(entry);
            }
        }

        void ApplyToCache(LogEntry<T> entry)
        {
            switch (entry.Operation)
            {
                case Operation.Write:
                    cache.AddOrUpdate(entry.Key, x => entry.Value, (z,y) => entry.Value);
                    deletedKeys.Remove(entry.Key);
                    break;
                case Operation.Delete:
                    T _ = default(T);
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

        public DeletableValue<T> GetValueOrDeleted(string key)
        {
            if (null == key) throw new ArgumentNullException(nameof(key));

            if (cache.ContainsKey(key))
            {
                return DeletableValue<T>.FromValue(cache[key]);
            }

            if (deletedKeys.Contains(key))
            {
                return DeletableValue<T>.FromDelete();
            }

            return null;
        }

        public void Del(string[] keys, bool requireFlush = false)
        {
            if (null == keys) throw new ArgumentNullException(nameof(keys));

            var deletes = keys.Select(x => LogEntry<T>.CreateDelete(x)).ToArray();

            this.Append(deletes, requireFlush);
            foreach (var delete in deletes)
            {
                this.ApplyToCache(delete);
                if (null != this.LogEvent) this.LogEvent(delete);
            }
        }

        public void Set(string key, T value, bool requireFlush = false)
        {
            var entry = LogEntry<T>.CreateWrite(key, value);
            this.Append(entry, requireFlush);
            ApplyToCache(entry);
            if (null != this.LogEvent) this.LogEvent(entry);
        }

        public void Set(KeyValue<T>[] records, bool requireFlush = false)
        {
            if (null == records) throw new ArgumentNullException(nameof(records));

            var entries = records.Select(x => LogEntry<T>.CreateWrite(x.Key, x.Value)).ToArray();
            this.Append(entries, requireFlush);
            foreach (var entry in entries)
            {
                ApplyToCache(entry);
                if (null != this.LogEvent) this.LogEvent(entry);
            }
        }

        internal IEnumerable<LogEntry<T>> Scan(string fromKey, string toKey)
        {
            foreach (var key in this.deletedKeys.Where(x => string.Compare(x, fromKey) >= 0).Where(x => string.Compare(x, toKey) < 0))
            {
                yield return LogEntry<T>.CreateDelete(key);
            }

            foreach (var entry in this.cache.Where(x => string.Compare(x.Key, fromKey) >= 0).Where(x => string.Compare(x.Key, toKey) < 0))
            {
                yield return LogEntry<T>.CreateWrite(entry.Key, entry.Value);
            }
        }

        public void Append(LogEntry<T> entry, bool requireFlush)
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

        public void Append(IEnumerable<LogEntry<T>> entries, bool requireFlush)
        {
            var buffer = entries.GetBuffer();
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
