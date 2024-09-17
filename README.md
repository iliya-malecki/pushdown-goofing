Here im trying to: 
- calculate stuff on the fly within a join node with a compute subnode shared between a join and a select
- allow projection pushdown to flow through the join
- as a result, enable things like `select magic_ai_similarity_score(t1.vector, t2.vector) from table t1 cross join table t2` to *not* grind to a halt due to endless data copying
