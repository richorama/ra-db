using RaDb.Index;
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
    public class Level<T> : IDisposable
    {
        Stream levelStream;
        //BloomFilter<string> filter;
        IBTree<string, long> index;
        ISerializer<T> serializer;

        public string Filename { get; private set; }

        public Level(string filename, ISerializer<T> serializer, IBTree<string, long> index = null)
        {
            if (null == filename) throw new ArgumentNullException(nameof(filename));
            this.Filename = filename;
            this.serializer = serializer;
            this.levelStream = new FileStream(filename, FileMode.OpenOrCreate);
            this.index = index ?? new BTree<string, long>();
            InitIndex();
        }

        public Level(Stream stream, string filename, ISerializer<T> serializer, IBTree<string,long> index = null)
        {
            if (null == stream) throw new ArgumentNullException(nameof(stream));
            this.Filename = filename;
            this.serializer = serializer;
            this.levelStream = stream;
            this.index = index ?? new BTree<string, long>();
            InitIndex();
        }

        void InitIndex()
        {
            //filter = new BloomFilter<string>(10000); // work this out
            
            foreach (var key in this.StreamAllKeys())
            {
                //filter.Add(key.Item1);
                index.Insert(key.Item1, key.Item2);
            }
        }

        public static Level<T> Build(Log<T> log, string filename, ISerializer<T> serializer)
        {
            var stream = new FileStream(filename, FileMode.OpenOrCreate);
            stream.Position = 0;
            foreach (var logEntry in StreamLogEntry(log).OrderBy(x => x.Key))
            {
                var buffer = logEntry.GetBuffer(serializer);
                stream.Write(buffer, 0, buffer.Length);
            }
            return new Level<T>(stream, filename, serializer);
        }

        public static Level<T> Compaction(IEnumerable<Level<T>> levels, string filename, ISerializer<T> serializer)
        {
            var dictionary = new Dictionary<string, T>();

            foreach (var level in levels)
            {
                foreach (var item in level.Scan())
                {
                    dictionary.ApplyOperation(item);
                }
            }

            var stream = new FileStream(filename, FileMode.OpenOrCreate);
            stream.Position = 0;
            foreach (var item in dictionary.OrderBy(x => x.Key).Select(x => LogEntry<T>.CreateWrite(x.Key, x.Value)))
            {
                var buffer = item.GetBuffer(serializer);
                stream.Write(buffer, 0, buffer.Length);
            }
            return new Level<T>(stream, filename, serializer);

        }

        static IEnumerable<LogEntry<T>> StreamLogEntry(Log<T> log)
        {
            foreach (var deletedKey in log.DeletedKeys)
            {
                yield return LogEntry<T>.CreateDelete(deletedKey);
            }

            foreach (var key in log.Keys)
            {
                var value = log.GetValueOrDeleted(key).Value;
                yield return LogEntry<T>.CreateWrite(key, value);
            }
        }

        public IEnumerable<LogEntry<T>> Scan()
        {
            return this.levelStream.ReadAll<T>(serializer);
        }

        public IEnumerable<LogEntry<T>> Scan(string from, string to)
        {
            // look up the position in the index
            var result = index.SearchNearest(from);
            
            if (null != result)
            {
                // less likely to get a match here, as from and to are speculative
                this.levelStream.Position = result.Pointer;
            }
            else
            {
                this.levelStream.Position = 0;
            }
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
                yield return new LogEntry<T>
                {
                    Key = readKey,
                    Value = valueSize == 0 ? default(T) : this.levelStream.ReadObject<T>(serializer,valueSize),
                    Operation = operation
                };
            }
        }


        IEnumerable<Tuple<string,long>> StreamAllKeys()
        {
            this.levelStream.Position = 0;
            while (this.levelStream.Position < this.levelStream.Length)
            {
                var position = this.levelStream.Position;
                var keySize = this.levelStream.ReadInt();
                var valueSize = this.levelStream.ReadInt();
                this.levelStream.Position += 1;
                yield return new Tuple<string,long>(this.levelStream.ReadString(keySize), position);
                this.levelStream.Position += valueSize;
            }
        }



        LogEntry<T> Find(string key)
        {
            // look up the position in the index
            var result = index.Search(key);
            if (null != result)
            {
                this.levelStream.Position = result.Pointer;
            }
            else
            {
                this.levelStream.Position = 0;
            }

            while (this.levelStream.Position < this.levelStream.Length)
            {
                // read header information 
                var keySize = this.levelStream.ReadInt();
                var valueSize = this.levelStream.ReadInt();
                var operation = (Operation)this.levelStream.ReadByte();
                var readKey = this.levelStream.ReadString(keySize);
                if (readKey == key)
                {
                    return new LogEntry<T>
                    {
                        Key = key,
                        Value = valueSize == 0 ? default(T) : this.levelStream.ReadObject<T>(serializer,valueSize),
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
      
        public DeletableValue<T> GetValueOrDeleted(string key)
        {
            //if (!this.filter.Contains(key)) return null;

            var value = this.Find(key);
            if (value == null) return null;
            if (value.Operation == Operation.Delete) return DeletableValue<T>.FromDelete();
            return DeletableValue<T>.FromValue(value.Value);
        }

        public void Dispose()
        {
            this.levelStream.Dispose();
        }
    }
}
