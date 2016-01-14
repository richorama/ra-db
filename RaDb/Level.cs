using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RaDb
{
    public class Level : IDisposable
    {
        Stream levelStream;
        BloomFilter<string> filter;

        public Level(string filename)
        {
            if (null == filename) throw new ArgumentNullException(nameof(filename));
            this.levelStream = new FileStream(filename, FileMode.OpenOrCreate);
            InitBloomFilter();
        }

        public Level(Stream stream)
        {
            if (null == stream) throw new ArgumentNullException(nameof(stream));
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

        public static Level Build(Log log, Stream stream)
        {
            stream.Position = 0;
            foreach (var logEntry in StreamLogEntry(log).OrderBy(x => x.Key))
            {
                var buffer = logEntry.GetBuffer();
                stream.Write(buffer, 0, buffer.Length);
            }
            return new Level(stream);
        }

        static IEnumerable<LogEntry> StreamLogEntry(Log log)
        {
            foreach (var deletedKey in log.DeletedKeys)
            {
                yield return LogEntry.CreateDelete(deletedKey);
            }

            foreach (var key in log.Keys)
            {
                var value = log.Get(key);
                yield return LogEntry.CreateWrite(key, value);
            }
        }

        public IEnumerable<LogEntry> Scan()
        {
            return this.levelStream.ReadAll();
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

        
        public string Get(string key)
        {
            if (!this.filter.Contains(key)) return null;

            var value = this.Find(key);
            if (value == null) return null;
            if (value.Operation == Operation.Delete) return null;
            return value.Value;
        }

        public DeletedResult GetValueOrDeleted(string key)
        {
            if (!this.filter.Contains(key)) return null;

            var value = this.Find(key);
            if (value == null) return null;
            if (value.Operation == Operation.Delete) return DeletedResult.FromDelete();
            return DeletedResult.FromValue(value.Value);
        }

        public void Dispose()
        {
            this.levelStream.Dispose();
        }
    }
}
