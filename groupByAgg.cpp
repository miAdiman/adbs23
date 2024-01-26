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

// ****************************************************************************
// Group-by operator with aggregation
// ****************************************************************************

void processHashtable(std::unordered_map<Row, int64_t*>& ht, const Relation* in, Relation* inKeys,
                      Relation* inVals, size_t numAggCols, AggFunc* aggFuncs, size_t start, size_t end){

    Row* keys = initRow(inKeys);
    Row* vals = initRow(inVals);

    for(size_t r = start; r < end; r++) {
      getRow(keys, inKeys, r);
      getRow(vals, inVals, r);
      // Search the key combination in the hash-table.
      int64_t*& accs = ht[*keys];
      if(accs) {
        // This key combination is already in the hash-table.
        // Update the accumulators.
        for(size_t c = 0; c < numAggCols; c++) {
          int64_t val = getValueInt64(vals, c);
          switch(aggFuncs[c]) {
            case AggFunc::SUM: accs[c] += val; break;
            case AggFunc::MIN: accs[c] = std::min(accs[c], val); break;
            case AggFunc::MAX: accs[c] = std::max(accs[c], val); break;
            default: exit(EXIT_FAILURE);
          }
        }
      }
      else {
        // This key combination is not in the hash-table yet.
        // Allocate and initialize the accumulators.
        accs = (int64_t*)(malloc(numAggCols * sizeof(int64_t*)));
        for(size_t c = 0; c < numAggCols; c++)
          accs[c] = getValueInt64(vals, c);
        keys->values = (void**)malloc(keys->numCols * sizeof(void*));
      }
    }

    freeRow(keys);
    freeRow(vals);
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
    std::unordered_map<Row, int64_t*> ht;

     //Iterate over the rows in the input relation, insert the tuples of keys
     //into the hash table while maintaining the accumulators for all aggregate
     //columns to create. Using threads based on hardware concurrency.
     //split up ht for threads
     //Multithreading
     int usedCores = NUM_CORES;

     size_t rowsPerThread;

     if (in->numRows < NUM_CORES) {
        usedCores = in->numRows;
        rowsPerThread = 1;
     }
     else {
        rowsPerThread = in->numRows / NUM_CORES;
     }

     std::vector<std::unordered_map<Row, int64_t*>> htVec(usedCores);
     std::vector<std::thread> threads(usedCores);

     for(int i = 0; i < usedCores; i++) {
        size_t start = i * rowsPerThread;
        size_t end = (i == usedCores - 1) ? in->numRows : start + rowsPerThread;
        threads[i] = std::thread(processHashtable, std::ref(htVec.at(i)), in, inKeys, inVals,
                                 numAggCols, aggFuncs, start, end);
     }

      // wait for all threads to finish
     for(auto& thread : threads){
        thread.join();
     }

     // merge all htVecs into one ht
    for(auto& htPiece : htVec){
      for(auto& entry : htPiece){
        auto it = ht.find(entry.first);
         if(it != ht.end()) {
    // Key already exists in htFinal, apply aggregate function
           for(size_t c = 0; c < numAggCols; c++) {
               switch(aggFuncs[c]) {
                 case AggFunc::SUM: it->second[c] += entry.second[c]; break;
                 case AggFunc::MIN: it->second[c] = std::min(it->second[c], entry.second[c]); break;
                 case AggFunc::MAX: it->second[c] = std::max(it->second[c], entry.second[c]); break;
                 default: exit(EXIT_FAILURE);
             }
           }
        }
       else {
          // Key doesn't exist in htFinal, insert entry
          ht[entry.first] = entry.second;
          }
        }
     }

    //baseline
    //Row* keys = initRow(inKeys);
    //Row* vals = initRow(inVals);
    //for(size_t r = 0; r < in->numRows; r++) {
    //  getRow(keys, inKeys, r);
    //  getRow(vals, inVals, r);
    //  // Search the key combination in the hash-table.
    //  int64_t*& accs = ht[*keys];
    //  if(accs) {
    //    // This key combination is already in the hash-table.
    //    // Update the accumulators.
    //    for(size_t c = 0; c < numAggCols; c++) {
    //      int64_t val = getValueInt64(vals, c);
    //      switch(aggFuncs[c]) {
    //        case AggFunc::SUM: accs[c] += val; break;
    //        case AggFunc::MIN: accs[c] = std::min(accs[c], val); break;
    //        case AggFunc::MAX: accs[c] = std::max(accs[c], val); break;
    //        default: exit(EXIT_FAILURE);
    //      }
    //    }
    //  }
    //  else {
    //    // This key combination is not in the hash-table yet.
    //    // Allocate and initialize the accumulators.
    //    accs = (int64_t*)(malloc(numAggCols * sizeof(int64_t*)));
    //    for(size_t c = 0; c < numAggCols; c++)
    //      accs[c] = getValueInt64(vals, c);
    //    keys->values = (void**)malloc(keys->numCols * sizeof(void*));
    //  }
    //}
    //freeRow(keys);
    //freeRow(vals);

    // Initialize the result relation.
    res->numRows = ht.size();
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
    size_t r = 0;
    Row* dst = initRow(res);
    for(auto entry : ht) {
        getRow(dst, res, r++);
        Row keys = entry.first;
        for(size_t c = 0; c < inKeys->numCols; c++)
            memcpy(dst->values[c], keys.values[c], sizeOfDataType(inKeys->colTypes[c]));
        free(keys.values);
        int64_t* accs = entry.second;
        for(size_t c = 0; c < inVals->numCols; c++) {
            memcpy(dst->values[numGrpCols + c], &accs[c], sizeOfDataType(res->colTypes[numGrpCols + c]));
        }
        free(accs);
    }
    freeRow(dst);

    freeRelation(inKeys, 1, 0);
    freeRelation(inVals, 1, 0);
}