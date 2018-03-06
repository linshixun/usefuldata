import org.apache.spark.sql.SparkSession

import scala.language.implicitConversions
val spark: SparkSession = SparkSession.builder().getOrCreate()

//    val list = List(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
//    val rdd = spark.sparkContext.parallelize(Seq(35, 86L, 11L, 10L, 12L)).flatMap(i => list.map(i * 10 + _)).flatMap(i => list.map(i * 10 + _)).flatMap(i => list.map(i * 10 + _)).flatMap(i => list.map(i * 10 + _)).flatMap(i => list.map(i * 10 + _)).flatMap(i => list.map(i * 10 + _)).flatMap(i => list.map(i * 10 + _)).flatMap(i => list.map(i * 10 + _)).repartition(100000).flatMap(i => list.map(i * 10 + _)).flatMap(i => list.map(i * 10 + _)).flatMap(i => list.map(i * 10 + _)).flatMap(i => list.map(i * 10 + _)).flatMap(i => list.map(i * 10 + _)).filter(i => (genCode(i)))


val list = List(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
val rdd = spark.sparkContext.parallelize(Seq(
  155L,
  132L,
  138L,
  133L,
  139L,
  184L,
  171L,
  183L,
  187L,
  188L,
  131L,
  130L,
  147L,
  136L,
  185L,
  177L,
  189L,
  135L,
  156L,
  176L,
  150L,
  170L,
  178L,
  153L,
  173L,
  180L,
  159L,
  158L,
  152L,
  186L,
  134L,
  149L,
  172L,
  181L,
  157L,
  182L,
  175L,
  137L,
  151L,
  145L,
  199L))
  .flatMap(i => list.map(i * 10 + _))
  .flatMap(i => list.map(i * 10 + _))
  .flatMap(i => list.map(i * 10 + _)).repartition(1000)
  .flatMap(i => list.map(i * 10 + _)).flatMap(i => list.map(i * 10 + _)).flatMap(i => list.map(i * 10 + _)).flatMap(i => list.map(i * 10 + _)).flatMap(i => list.map(i * 10 + _))
