using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace RaDb
{
    public class Database : IDisposable
    {
        const int MAX_LOG_SIZE = 4 * 1024 * 1024; // 4MB

        public DirectoryInfo DbDirectory { get; private set; }

        public int CurrentLevelNumber { get; private set; }

        public Database(string path)
        {
            this.DbDirectory = Directory.CreateDirectory(path);
            this.Levels = new List<Level>();

            foreach (var file in this.DbDirectory.EnumerateFiles().OrderBy(x => x.Name))
            {
                switch (file.Extension)
                {
                    case ".log":
                        this.ActiveLog = new Log(file.FullName);
                        break;
                    case ".level":
                        this.Levels.Add(new Level(file.FullName));
                        this.CurrentLevelNumber = int.Parse(file.Name.Replace(".level", ""));
                        break;
                }
            }
            if (null == this.ActiveLog)
            {
                this.ActiveLog = new Log(Path.Combine(this.DbDirectory.FullName, "database.log"));
            }
        }

        public Log ActiveLog { get; private set; }
        public List<Level> Levels { get; private set; }

        public string Get(string key)
        {
            var result = this.ActiveLog.GetValueOrDeleted(key);
            if (null != result)
            {
                if (result.IsDeleted) return null;
                return result.Value;
            }

            for (var i = this.Levels.Count - 1; i >= 0; i--)
            {
                var levelResult = this.Levels[i].GetValueOrDeleted(key);
                if (null == levelResult) continue;
                if (levelResult.IsDeleted) return null;
                return levelResult.Value;
            }
            return null;
        }

        public void Set(string key, string value, bool requireDiskWrite = false)
        {
            this.ActiveLog.Set(key, value, requireDiskWrite);
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

        void LevelUp()
        {
            try
            {
                Monitor.Enter(this.ActiveLog);
                if (this.ActiveLog.Size <= MAX_LOG_SIZE) return;
               
                var stream = new FileStream(Path.Combine(this.DbDirectory.FullName, $"{this.CurrentLevelNumber.ToString("D4")}.level"), FileMode.CreateNew);
                this.CurrentLevelNumber++;
                var level = Level.Build(this.ActiveLog, stream);
                this.Levels.Add(level);
                this.ActiveLog.Clear();
            }
            finally
            {
                Monitor.Exit(this.ActiveLog);
            }
        }


        public IEnumerable<KeyValue> Search(string fromKey, string toKey)
        {
            var results = new Dictionary<string, string>();
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

            return results.Select(x => new KeyValue { Key = x.Key, Value = x.Value }).OrderBy(x => x.Key);
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
