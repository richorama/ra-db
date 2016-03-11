using System.IO;

namespace RaDb
{
    public interface ISerializer<T>
    {
        T Deserialize(Stream stream, int length);
        byte[] Serialize(T value, out int length);
    }
}