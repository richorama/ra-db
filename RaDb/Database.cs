﻿using System;
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
        const int MAX_LEVELS = 10;

        public DirectoryInfo DbDirectory { get; private set; }

        public int CurrentLevelNumber { get; private set; }

        public bool HitDisk { get; private set; }

        public ISerializer<T> Serializer { get; private set; }

        public Database(string path, bool requireWritesToHitDisk = true, ISerializer<T> serializer = null)
        {
            if (null == path) throw new ArgumentNullException(nameof(path));

            this.HitDisk = requireWritesToHitDisk;
            this.DbDirectory = Directory.CreateDirectory(path);
            this.Levels = new List<Level<T>>();
            this.Serializer = serializer ?? new Serializer<T>();

            foreach (var file in this.DbDirectory.EnumerateFiles().OrderBy(x => x.Name))
            {
                switch (file.Extension)
                {
                    case ".log":
                        this.ActiveLog = new Log<T>(file.FullName, this.Serializer);
                        break;
                    case ".level":
                        this.Levels.Add(new Level<T>(file.FullName, this.Serializer));
                        this.CurrentLevelNumber = int.Parse(file.Name.Replace(".level", ""));
                        break;
                }
            }

            // add one to the level number, so we're ready to create the next level.
            this.CurrentLevelNumber++;

            if (null == this.ActiveLog)
            {
                this.ActiveLog = new Log<T>(Path.Combine(this.DbDirectory.FullName, "database.log"), this.Serializer);
            }
        }

        public Log<T> ActiveLog { get; private set; }
        public List<Level<T>> Levels { get; private set; }

        public T Get(string key)
        {
            if (null == key) throw new ArgumentNullException(nameof(key));

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


        public void Set(string key, T value)
        {
            if (null == key) throw new ArgumentNullException(nameof(key));

            this.SetMulti(new KeyValue<T>(key, value));
        }

        public void SetMulti(params KeyValue<T>[] values)
        {
            if (null == values) throw new ArgumentNullException(nameof(values));

            this.ActiveLog.Set(values, this.HitDisk);
            if (this.ActiveLog.Size > MAX_LOG_SIZE)
            {
                this.LevelUp();
            }
        }


        public void Del(params string[] keys)
        {
            if (null == keys) throw new ArgumentNullException(nameof(keys));

            this.ActiveLog.Del(keys, this.HitDisk);
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


        public void LevelUp(bool force = false)
        {
            try
            {
                // TODO: Figure out if some of this could be done in a background task?

                Monitor.Enter(this.ActiveLog);
                if (this.ActiveLog.Size <= MAX_LOG_SIZE && !force) return;
               
                var filename = this.NextLevelName();
                var level = Level<T>.Build(this.ActiveLog, filename, Serializer);
                this.Levels.Add(level);
                this.ActiveLog.Clear();

                if (this.Levels.Count > MAX_LEVELS)
                {
                    Compact();
                }

            }
            finally
            {
                Monitor.Exit(this.ActiveLog);
            }
        }

        public void Compact()
        {
            var compactedFilename = this.NextLevelName();
            var newLevel = Level<T>.Compaction(this.Levels, compactedFilename, Serializer);
            var oldLevels = this.Levels.ToArray();
            this.Levels.Clear();
            this.Levels.Add(newLevel);
            foreach (var oldLevel in oldLevels)
            {
                oldLevel.Dispose();
                File.Delete(oldLevel.Filename);
            }
        }


        public IEnumerable<KeyValue<T>> Between(string fromKey, string toKey)
        {
            if (null == fromKey) throw new ArgumentNullException(nameof(fromKey));
            if (null == toKey) throw new ArgumentNullException(nameof(toKey));

            var ignoreList = new HashSet<string>();

            foreach (var item in this.ActiveLog.Scan(fromKey, toKey))
            {
                ignoreList.Add(item.Key);
                if (item.Operation == Operation.Delete) continue;
                yield return new KeyValue<T>(item.Key, item.Value);
            }

            for (var i = this.Levels.Count - 1; i >= 0; i--)
            {
                var level = this.Levels[i];
                foreach (var item in level.Scan(fromKey, toKey))
                {
                    if (ignoreList.Contains(item.Key)) continue;
                    ignoreList.Add(item.Key);
                    if (item.Operation == Operation.Delete) continue;
                    yield return new KeyValue<T>(item.Key, item.Value);
                }                   
            }
        
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
