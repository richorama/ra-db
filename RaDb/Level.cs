using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RaDb
{
    /// <summary>
    /// Levels are immutable snapshots of the database.
    /// There can be multiple levels, higher levels override lower levels
    /// Levels can contain deletes
    /// Levels should contain only one instance of a key
    /// Caching, and key bounds could be added to improve read performance
    /// </summary>
    public class Level : IDisposable
    {
        Stream levelStream;
        BloomFilter<string> filter;

        public string Filename { get; private set; }

        public Level(string filename)
        {
            if (null == filename) throw new ArgumentNullException(nameof(filename));
            this.Filename = filename;
            this.levelStream = new FileStream(filename, FileMode.OpenOrCreate);
            InitBloomFilter();
        }

        public Level(Stream stream, string filename)
        {
            if (null == stream) throw new ArgumentNullException(nameof(stream));
            this.Filename = filename;
            this.levelStream = stream;
            InitBloomFilter();
        }

        void InitBloomFilter()
        {
            filter = new BloomFilter<string>(10000); // work this out
            foreach (var key in this.StreamAllKeys())
            {
                filter.Add(key);
            }
        }

        public static Level Build(Log log, string filename)
        {
            var stream = new FileStream(filename, FileMode.OpenOrCreate);
            stream.Position = 0;
            foreach (var logEntry in StreamLogEntry(log).OrderBy(x => x.Key))
            {
                var buffer = logEntry.GetBuffer();
                stream.Write(buffer, 0, buffer.Length);
            }
            return new Level(stream, filename);
        }

        public static Level Compaction(IEnumerable<Level> levels, string filename)
        {
            var dictionary = new Dictionary<string, string>();

            foreach (var level in levels)
            {
                foreach (var item in level.Scan())
                {
                    dictionary.ApplyOperation(item);
                }
            }

            var stream = new FileStream(filename, FileMode.OpenOrCreate);
            stream.Position = 0;
            foreach (var item in dictionary.OrderBy(x => x.Key).Select(x => LogEntry.CreateWrite(x.Key, x.Value)))
            {
                var buffer = item.GetBuffer();
                stream.Write(buffer, 0, buffer.Length);
            }
            return new Level(stream, filename);

        }

        static IEnumerable<LogEntry> StreamLogEntry(Log log)
        {
            foreach (var deletedKey in log.DeletedKeys)
            {
                yield return LogEntry.CreateDelete(deletedKey);
            }

            foreach (var key in log.Keys)
            {
                var value = log.GetValueOrDeleted(key).Value;
                yield return LogEntry.CreateWrite(key, value);
            }
        }

        public IEnumerable<LogEntry> Scan()
        {
            return this.levelStream.ReadAll();
        }

        public IEnumerable<LogEntry> Scan(string from, string to)
        {
            this.levelStream.Position = 0;
            while (this.levelStream.Position < this.levelStream.Length)
            {
                // read header information 
                var keySize = this.levelStream.ReadInt();
                var valueSize = this.levelStream.ReadInt();
                var operation = (Operation)this.levelStream.ReadByte();
                var readKey = this.levelStream.ReadString(keySize);

                if (string.Compare(readKey, from) < 0)
                {
                    // not hit the key range yet
                    this.levelStream.Position += valueSize;
                    continue;
                }

                if(string.Compare(readKey, to) >= 0)
                {
                    // gone past the key range
                    yield break;
                }

                // within the key range
                yield return new LogEntry
                {
                    Key = readKey,
                    Value = this.levelStream.ReadString(valueSize),
                    Operation = operation
                };
            }
        }

        IEnumerable<string> StreamAllKeys()
        {
            this.levelStream.Position = 0;
            while (this.levelStream.Position < this.levelStream.Length)
            {
                var keySize = this.levelStream.ReadInt();
                var valueSize = this.levelStream.ReadInt();
                this.levelStream.Position += 1;
                yield return this.levelStream.ReadString(keySize);
                this.levelStream.Position += valueSize;
            }
        }

        LogEntry Find(string key)
        {
            this.levelStream.Position = 0;
            while (this.levelStream.Position < this.levelStream.Length)
            {
                // read header information 
                var keySize = this.levelStream.ReadInt();
                var valueSize = this.levelStream.ReadInt();
                var operation = (Operation)this.levelStream.ReadByte();
                var readKey = this.levelStream.ReadString(keySize);
                if (readKey == key)
                {
                    return new LogEntry
                    {
                        Key = key,
                        Value = this.levelStream.ReadString(valueSize),
                        Operation = operation
                    };
                }
                
                if (string.Compare(readKey, key) > 0)
                {
                    // the keys are sorted, so if you have read to a greater key value you have missed the key
                    return null;
                }

                // skip the value
                this.levelStream.Position += valueSize;
            }
            return null;
        }
      
        public DeletableValue GetValueOrDeleted(string key)
        {
            if (!this.filter.Contains(key)) return null;

            var value = this.Find(key);
            if (value == null) return null;
            if (value.Operation == Operation.Delete) return DeletableValue.FromDelete();
            return DeletableValue.FromValue(value.Value);
        }

        public void Dispose()
        {
            this.levelStream.Dispose();
        }
    }
}
