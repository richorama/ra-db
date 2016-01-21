using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace RaDb
{
    public class Database<T> : IDisposable
    {
        const int MAX_LOG_SIZE = 4 * 1024 * 1024; // 4MB
        const int MAX_LEVELS = 4;

        public DirectoryInfo DbDirectory { get; private set; }

        public int CurrentLevelNumber { get; private set; }

        public Database(string path)
        {
            if (null == path) throw new ArgumentNullException(nameof(path));

            this.DbDirectory = Directory.CreateDirectory(path);
            this.Levels = new List<Level<T>>();

            foreach (var file in this.DbDirectory.EnumerateFiles().OrderBy(x => x.Name))
            {
                switch (file.Extension)
                {
                    case ".log":
                        this.ActiveLog = new Log<T>(file.FullName);
                        break;
                    case ".level":
                        this.Levels.Add(new Level<T>(file.FullName));
                        this.CurrentLevelNumber = int.Parse(file.Name.Replace(".level", ""));
                        break;
                }
            }

            // add one to the level number, so we're ready to create the next level.
            this.CurrentLevelNumber++;

            if (null == this.ActiveLog)
            {
                this.ActiveLog = new Log<T>(Path.Combine(this.DbDirectory.FullName, "database.log"));
            }
        }

        public Log<T> ActiveLog { get; private set; }
        public List<Level<T>> Levels { get; private set; }

        public T Get(string key)
        {
            var result = this.ActiveLog.GetValueOrDeleted(key);
            if (null != result)
            {
                if (result.IsDeleted) return default(T);
                return result.Value;
            }

            for (var i = this.Levels.Count - 1; i >= 0; i--)
            {
                var levelResult = this.Levels[i].GetValueOrDeleted(key);
                if (null == levelResult) continue;
                if (levelResult.IsDeleted) return default(T);
                return levelResult.Value;
            }
            return default(T);
        }

        public void Set(string key, T value, bool requireDiskWrite = false)
        {
            this.ActiveLog.Set(key, value, requireDiskWrite);
            if (this.ActiveLog.Size > MAX_LOG_SIZE)
            {
                this.LevelUp();
            }
        }

        public void Set(IEnumerable<KeyValue<T>> values, bool requireDiskWrite = false)
        {
            this.ActiveLog.Set(values, requireDiskWrite);
            if (this.ActiveLog.Size > MAX_LOG_SIZE)
            {
                this.LevelUp();
            }
        }


        public void Del(string key, bool requireDiskWrite = false)
        {
            this.ActiveLog.Del(key, requireDiskWrite);
            if (this.ActiveLog.Size > MAX_LOG_SIZE)
            {
                this.LevelUp();
            }
        }

        string NextLevelName()
        {
            var path = Path.Combine(this.DbDirectory.FullName, $"{this.CurrentLevelNumber.ToString("D4")}.level");
            this.CurrentLevelNumber++;
            return path;
        }


        void LevelUp()
        {
            try
            {
                // TODO: Figure out if some of this could be done in a background task?

                Monitor.Enter(this.ActiveLog);
                if (this.ActiveLog.Size <= MAX_LOG_SIZE) return;
               
                var filename = this.NextLevelName();
                var level = Level<T>.Build(this.ActiveLog, filename);
                this.Levels.Add(level);
                this.ActiveLog.Clear();

                if (this.Levels.Count > MAX_LEVELS)
                {
                    // we've hit the max numer of levels, so consolidate down to a single level
                    // compactions are extremely experimental!!!
                    var compactedFilename = this.NextLevelName();
                    var newLevel = Level<T>.Compaction(this.Levels, compactedFilename);
                    var oldLevels = this.Levels.ToArray();
                    this.Levels.Clear();
                    this.Levels.Add(newLevel);
                    foreach (var oldLevel in oldLevels)
                    {
                        oldLevel.Dispose();
                        File.Delete(oldLevel.Filename);
                    }
                }

            }
            finally
            {
                Monitor.Exit(this.ActiveLog);
            }
        }


        public IEnumerable<KeyValue<T>> Search(string fromKey, string toKey)
        {
            if (null == fromKey) throw new ArgumentNullException(nameof(fromKey));
            if (null == toKey) throw new ArgumentNullException(nameof(toKey));

            var results = new Dictionary<string, T>();
            foreach (var level in this.Levels)
            {
                foreach (var item in level.Scan(fromKey, toKey))
                {
                    results.ApplyOperation(item);
                }
            }

            foreach (var item in this.ActiveLog.Scan(fromKey, toKey))
            {
                results.ApplyOperation(item);
            }

            return results.Select(x => new KeyValue<T> { Key = x.Key, Value = x.Value }).OrderBy(x => x.Key);
        }

        public void Dispose()
        {
            foreach (var level in this.Levels)
            {
                level.Dispose();
            }
            this.ActiveLog.Dispose();
        }
    }
}
