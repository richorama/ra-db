# RaDb

An experiment in .NET databases inspired by LevelDB.

__DO NOT USE__

## Usage

```c#

using (var db = new Database<string>("."))
{
	// write values
	db.Set("key1", "value1");
	db.Set("key2", "value2");

	// get values
	db.Get("key2"); // "value2"

	// delete values
	db.Del("key1");

	// search within a key range
	var results = db.Search("from_this_key", "to_this_key");
	foreach (var item in results)
	{
		Console.WriteLine($"{item.Key} = {item.Value}");
	}
}

```

## Todo

* Think about isolation during search and compaction
* Perf perf perf
* Implement a cache on the levels (perhaps after the bloom filter?)
* Look at an alternative binary serializer
* Run distributed with raft
* Add an HTTP API
* ~~Support batching~~

## License

MIT
