# RaDb

An experiment in .NET databases inspired by LevelDB.

__DO NOT USE__

## Usage

If you must use this database, you can do so like this:

```c#
// declare a poco class

public class MyValue
{
	public string StringValue { get; set; }
}


using (var db = new Database<MyValue>("."))
{
	// write values
	db.Set("key1", new MyValue { StringValue = "value1" });
	db.Set("key2", new MyValue { StringValue = "value2" });

	// get values
	db.Get("key2"); // { StringValue = "value2" }

	// delete values
	db.Del("key1");

	// search within a key range
	var results = db.Between("from_this_key", "to_this_key");
	foreach (var item in results)
	{
		Console.WriteLine($"{item.Key} = {item.Value.StringValue}");
	}
}

```

## Todo

* Think about isolation during search and compaction
* Perf perf perf
* ~~Implement a cache on the levels~~
* ~~Add an index~~
* Add secondary indexes
* ~~Look at an alternative binary serializer~~
* Run distributed with raft
* Add an HTTP API
* ~~Support batching~~

## License

MIT
