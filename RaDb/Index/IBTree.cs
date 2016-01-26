using System;

namespace RaDb.Index
{
    public interface IBTree<TK, TP> where TK : IComparable<TK>
    {
        void Delete(TK keyToDelete);
        void Insert(TK newKey, TP newPointer);
        Entry<TK, TP> Search(TK key);
        Entry<TK, TP> SearchNearest(TK key);
    }
}