using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RaDb
{
    public static class ExtensionMethods
    {
        public static byte[] GetBytes(this string str)
        {
            //var bytes = new byte[str.Length * sizeof(char)];
            //Buffer.BlockCopy(str.ToCharArray(), 0, bytes, 0, bytes.Length);
            return Encoding.Default.GetBytes(str);
            //return bytes;
        }

        public static string GetString(this byte[] bytes)
        {
            return Encoding.Default.GetString(bytes);
            /*
            var chars = new char[bytes.Length / sizeof(char)];
            Buffer.BlockCopy(bytes, 0, chars, 0, bytes.Length);
            return new string(chars);*/
        }

        public static int ReadInt(this Stream stream)
        {
            var buffer = new byte[sizeof(int)];
            stream.Read(buffer, 0, sizeof(int));
            return BitConverter.ToInt32(buffer,0);
        }

        public static string ReadString(this Stream stream, int length)
        {
            var buffer = new byte[length];
            stream.Read(buffer, 0, length);
            return buffer.GetString();
        }
    }
}
