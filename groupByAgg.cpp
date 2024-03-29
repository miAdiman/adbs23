// Students: Implement your solution in this file. Initially, it is a copy of
//           groupByAggBaseline.cpp.

#include "api.h"
#include "setmap_utils.h"
#include "utils.h"
#include "thread"
#include "hardware.h"

#include <functional>
#include <unordered_map>

#include <cstring>

#define GOLDEN_RATIO 0x9e3779b9

class Hashtable {
  private:
    struct Node {
        Row key_;
        int64_t* value_;
        //bool tombstone_;
        size_t keyHash_;

        Node(const Row& key, int64_t* value, size_t keyHash) : key_(key), value_(value), keyHash_(keyHash) {}
    };

    size_t tableSize_;
    size_t elementAmount_;

    static constexpr double loadFactorThreshold = 0.80;
    void resize();


  public:
    std::vector<Node*> table_;
    Hashtable(size_t size) : tableSize_(size > 5 ? size : 5), elementAmount_(0) {
        table_.resize(tableSize_, nullptr);
    }

    ~Hashtable() {
        for (auto& node : table_) {
            delete node;
        }
    }

    size_t hash(const Row& key) {
        //size_t result = 0;
        size_t hash = 0;
        for(size_t c = 0; c < key.numCols; c++) {
            hash += getValueInt64(&key, c);
        }
        return hash;
        // result = std::hash<Row>()(key);
        // return result;
    }

    void insert(const Row& key, int64_t* value, size_t idx, size_t keyHash);
    //void remove(const Row& key);
    int64_t*& operator[](const Row& key);
    size_t size() const {
        return elementAmount_;
    }
    bool compareRows(const Row& row1, const Row& row2, size_t hashRow1, size_t hashRow2);

    size_t getTableSize() const {
        return tableSize_;
    }
};

void Hashtable::insert(const Row& key, int64_t* value, size_t idx, size_t keyHash) {
    table_[idx] = new Node(key, value, keyHash);
    elementAmount_++;

    if (static_cast<double>(elementAmount_) / tableSize_ > loadFactorThreshold) {
        resize();
    }

}

//void Hashtable::remove(const Row& key) {
//    size_t index = hash(key) % tableSize_;
//    // increment until key is found
//    while(table_[index] != nullptr && (table_[index]->tombstone_ || !(table_[index]->key_ == key))) {
//        index = (index + 1) % tableSize_;
//    }
//
//    // check if key is found
//    if(table_[index] != nullptr) {
//        table_[index]->tombstone_ = true;
//        elementAmount_--;
//    }
//}

bool Hashtable::compareRows(const Row& row1, const Row& row2, size_t hashRow1, size_t hashRow2) {
    if(hashRow1 != hashRow2)
        return false;
    // Check number of columns.
    if(row1.numCols != row2.numCols)
        return false;
    // Check column types.
    if(row1.types != row2.types)
        for(size_t c = 0; c < row1.numCols; c++)
            if(row1.types[c] != row2.types[c])
                return false;
    // Check stored values.
    for(size_t c = 0; c < row1.numCols; c++)
        if(getValueInt64(&row1, c) != getValueInt64(&row2, c))
            return false;
    // All checks succeeded.
    return true;
}

int64_t*& Hashtable::operator[](const Row& key) {
    size_t keyHash = hash(key);
    size_t index = keyHash % tableSize_;
    // increment until key is found

    while(table_[index] != nullptr && !compareRows(key, table_[index]->key_, keyHash, table_[index]->keyHash_)) {
        index = (index + 1) % tableSize_;
    }
    // check if key is found
    if(table_[index] != nullptr) {
        return table_[index]->value_;
    }
    // if key is not found, insert it
    else {
        insert(key, nullptr, index, keyHash);
        return table_[index]->value_;
    }
}

void Hashtable::resize() {
    // create new table with double size
    std::vector<Node*> newTable;
    newTable.resize(tableSize_ * 2, nullptr);

    // rehash all elements
    for (auto& node : table_) {
        if (node != nullptr) {
            size_t index = node->keyHash_ % (tableSize_ * 2);
            while (newTable[index] != nullptr) {
                index = (index + 1) % (tableSize_ * 2);
            }
            newTable[index] = node;
        }
    }
    table_ = newTable;
    tableSize_ *= 2;
}

// ****************************************************************************
// Group-by operator with aggregation
// ****************************************************************************

// Hashtable& ht
void processHashtable(Hashtable& ht, Relation* inKeys,
                      Relation* inVals, size_t numAggCols, AggFunc* aggFuncs, size_t start, size_t end){

    Row* keys = initRow(inKeys);
    Row* vals = initRow(inVals);

    for(size_t i = start; i < end; i++) {
        getRow(keys, inKeys, i);
        getRow(vals, inVals, i);
        // Search the key combination in the hash-table.
        int64_t*& accs = ht[*keys];
        if(accs) {
            // This key combination is already in the hash-table.
            // Update the accumulators.
            for(size_t j = 0; j < numAggCols; j++) {
                int64_t val = getValueInt64(vals, j);
                switch(aggFuncs[j]) {
                    case AggFunc::SUM: accs[j] += val; break;
                    case AggFunc::MIN: accs[j] = std::min(accs[j], val); break;
                    case AggFunc::MAX: accs[j] = std::max(accs[j], val); break;
                    default: exit(EXIT_FAILURE);
                }
            }
        }
        else {
            // This key combination is not in the hash-table yet.
            // Allocate and initialize the accumulators.
            accs = (int64_t*)(malloc(numAggCols * sizeof(int64_t*)));
            for(size_t j = 0; j < numAggCols; j++)
                accs[j] = getValueInt64(vals, j);
            keys->values = (void**)malloc(keys->numCols * sizeof(void*));
        }
    }

    freeRow(keys);
    freeRow(vals);
}

void mergeHashTables(std::vector<Hashtable>& htVec, Hashtable& ownHt, size_t numAggCols, AggFunc* aggFuncs) {
    for (auto &htPiece: htVec) {
        for (auto node: htPiece.table_) {
            if (node != nullptr) {
                size_t hash = ownHt.hash(node->key_);
                size_t index = hash % ownHt.getTableSize();
                // increment until key is found or an empty slot is encountered
                while(ownHt.table_[index] != nullptr && !ownHt.compareRows(ownHt.table_[index]->key_, node->key_, ownHt.table_[index]->keyHash_, hash)) {
                    index = (index + 1) % ownHt.getTableSize();
                }
                auto it = ownHt.table_[index];
                if (it != nullptr) {
                    // Key already exists in ownHt, apply aggregate function
                    for (size_t c = 0; c < numAggCols; c++) {
                        switch (aggFuncs[c]) {
                            case AggFunc::SUM:
                                it->value_[c] += node->value_[c];
                                break;
                            case AggFunc::MIN:
                                it->value_[c] = std::min(it->value_[c], node->value_[c]);
                                break;
                            case AggFunc::MAX:
                                it->value_[c] = std::max(it->value_[c], node->value_[c]);
                                break;
                            default:
                                exit(EXIT_FAILURE);
                        }
                    }
                } else {
                    // Key doesn't exist in ownHt, insert entry
                    ownHt.insert(node->key_, node->value_, index, hash);
                }
            }
        }
    }
}


/* Student implementation of the `groupByAgg`-operator */
void groupByAgg(
        Relation* res,
        const Relation* in,
        size_t numGrpCols, size_t* grpColIdxs,
        size_t numAggCols, size_t* aggColIdxs, AggFunc* aggFuncs
) {
    // Split the input relation into key and value columns, such that we can
    // easily extract rows of key and value columns (no copying involved).
    Relation* inKeys = project(in, numGrpCols, grpColIdxs);
    Relation* inVals = project(in, numAggCols, aggColIdxs);

    // A hash-table for the hash-based grouping.
    //std::unordered_map<Row, int64_t*> ht;
    Hashtable ownHt(in->numRows);

    //Iterate over the rows in the input relation, insert the tuples of keys
    //into the hash table while maintaining the accumulators for all aggregate
    //columns to create. Using threads based on hardware concurrency.
    //split up ht for threads

    //Multithreading
    bool mt = true;
    if(mt) {
        int usedCores = NUM_CORES;

        size_t rowsPerThread;

        if (in->numRows < NUM_CORES) {
            usedCores = in->numRows;
            rowsPerThread = 1;
        } else {
            rowsPerThread = in->numRows / NUM_CORES;
        }

        //std::vector<std::unordered_map<Row, int64_t*>> htVec(rowsPerThread);
        std::vector<Hashtable> htVec(usedCores, Hashtable(rowsPerThread));
        std::vector<std::thread> threads(usedCores);


        for (int i = 0; i < usedCores; i++) {
            size_t start = i * rowsPerThread;
            size_t end = (i == usedCores - 1) ? in->numRows : start + rowsPerThread;
            threads[i] = std::thread(processHashtable, std::ref(htVec.at(i)), inKeys, inVals,
                                     numAggCols, aggFuncs, start, end);
        }

        // wait for all threads to finish
        for (auto &thread: threads) {
            thread.join();
        }

        // merge all htVecs into one ht, aggregating if key already exists
        //        for (auto &htPiece: htVec) {
        //            for (auto entry : htPiece) {
        //            Row key = entry.first;
        //            int64_t*& accs = ht[key];
        //            if(accs) {
        //                // This key combination is already in the hash-table.
        //                // Update the accumulators.
        //                for(size_t j = 0; j < numAggCols; j++) {
        //                int64_t val = entry.second[j];
        //                switch(aggFuncs[j]) {
        //                    case AggFunc::SUM: accs[j] += val; break;
        //                    case AggFunc::MIN: accs[j] = std::min(accs[j], val); break;
        //                    case AggFunc::MAX: accs[j] = std::max(accs[j], val); break;
        //                    default: exit(EXIT_FAILURE);
        //                }
        //                }
        //            }
        //            else {
        //                // This key combination is not in the hash-table yet.
        //                // Allocate and initialize the accumulators.
        //                accs = (int64_t*)(malloc(numAggCols * sizeof(int64_t*)));
        //                for(size_t j = 0; j < numAggCols; j++)
        //                    accs[j] = entry.second[j];
        //                key.values = (void**)malloc(key.numCols * sizeof(void*));
        //            }
        //            }
        //        }

        // merge all htVecs into one ht
        mergeHashTables(htVec, ownHt, numAggCols, aggFuncs);
    }
    else {
        processHashtable(ownHt, inKeys, inVals, numAggCols, aggFuncs, 0, in->numRows);
    }

    // Initialize the result relation.
    res->numRows = ownHt.size();
    res->numCols = numGrpCols + numAggCols;
    res->colTypes = (DataType*)malloc(res->numCols * sizeof(DataType));
    res->cols = (void**)malloc(res->numCols * sizeof(void*));
    for(size_t c = 0; c < numGrpCols; c++) {
        res->colTypes[c] = inKeys->colTypes[c];
        res->cols[c] = (void*)malloc(res->numRows * sizeOfDataType(res->colTypes[c]));
    }
    for(size_t c = 0; c < numAggCols; c++) {
        res->colTypes[numGrpCols + c] = getAggType(inVals->colTypes[c], aggFuncs[c]);
        res->cols[numGrpCols + c] = (void*)malloc(res->numRows * sizeOfDataType(res->colTypes[numGrpCols + c]));
    }
    // Populate the result with the data from the hash-table.
    //    size_t r = 0;
    //    Row* dst = initRow(res);
    //    for(auto entry : ht) {
    //        getRow(dst, res, r++);
    //        Row keys = entry.first;
    //        for(size_t c = 0; c < inKeys->numCols; c++)
    //            memcpy(dst->values[c], keys.values[c], sizeOfDataType(inKeys->colTypes[c]));
    //        free(keys.values);
    //        int64_t* accs = entry.second;
    //        for(size_t c = 0; c < inVals->numCols; c++) {
    //            memcpy(dst->values[numGrpCols + c], &accs[c], sizeOfDataType(res->colTypes[numGrpCols + c]));
    //        }
    //        free(accs);
    //    }
    //    freeRow(dst);

    size_t r = 0;
    Row* dst = initRow(res);
    for(auto node: ownHt.table_) {
        if(node != nullptr) {
            getRow(dst, res, r++);
            Row keys = node->key_;
            for(size_t c = 0; c < inKeys->numCols; c++)
                memcpy(dst->values[c], keys.values[c], sizeOfDataType(inKeys->colTypes[c]));
            free(keys.values);
            int64_t* accs = node->value_;
            for(size_t i = 0; i < inVals->numCols; i++) {
                memcpy(dst->values[numGrpCols + i], &accs[i], sizeOfDataType(res->colTypes[numGrpCols + i]));
            }
            free(accs);
        }
    }
    freeRow(dst);

    freeRelation(inKeys, 1, 0);
    freeRelation(inVals, 1, 0);
}