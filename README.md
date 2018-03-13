




# Set-Similarity Joins using mapreduce

Finding pairs of records with a similarity on their join attributes which is great/equal than a given threshold
In this project we use **Jaccard similarity**:
```
Sim(r1,r2) = |r1∩r2| / |r1∪r2|
```
the input file is given,in which each line is format of:
			"RecordId List<ElementId>"   *(intergers are separated by space)* 
	
e.g.

	```
	0 1 2 3 4 5
	1 2 4 5 8
	2 3 4 5
	```
 List<ElementId> in which ElementIds are sorted in ascending order by their frequency.
  
  
  # Solution
  
  For this project, a two-stage mapreduce was implemented.
  
 **Stage 1:Find similar id pairs**
 for each record that can have smimilarity > certain threshold,we introduce a prefix length.
  ```
   As for Sim(r1,r2) >= threshold -> |r1∩r2| >= |r1∪r2| * threshold => Max(|r1|,|r2|) * threshold
  ```
  Therefore for every record **r**, if there exist an another record that the similarity between these two records.they must share at least one token in the prefix length.
  ```
  prefix length = |r| - |r| * threshold + 1
  ```
  In the program for a given record r = (A,B,C,D) and prefix length = 2, the mapper emits(A,r) and (B,r)
  
  and in the reducer we simply compute similarity for each key,only keep the pair that their similarity > threshold and emits(r1Id,r2Id,similarity) with r1Id < r2Id
  
 **Stage 2:Remove Duplicates**
 
 In the stage 2 just simply handle the duplicate value which just simply take the first value in the reducer as all the same key is send to the same reducer.
 
	
