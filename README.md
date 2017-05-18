# Feature Engineering with Outbrain Ad Display Data
By: Hee  

[Go to blog post](https://heekyungyoon.github.io/2017-05-16/thread-safe-multi-processing/)

This code tests multi threading in feature engineering with large size data (80GB, 2 billion rows).
It solved race condition problem in multi threading by completely separating object that each thread updates. 

- Create sparse matrix for user-topic and document-topic
  - Create document-topic matrix from document-topic data
  - (Multi threading) Create user-topic matrix by joining user-document data and document-topic matrix (Implemented user-topic matrix as a set of matrices)
- Loop over user-topic and find matching document-topic to calculate interaction score between user and document

## Quickstart
```$sh
$ g++ -std=c++11 main.cpp io.h -o main -lboost_iostreams -lpthread
$ ./main
```

