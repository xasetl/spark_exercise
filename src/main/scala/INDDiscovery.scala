import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.rdd.RDD
import org.apache.log4j.{Level, Logger}
import scala.collection.mutable.ListBuffer
import java.io.File

object INDDiscovery extends App {

  override def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // ugly handling of command-line arguments
    var inputPath = "TPCH"
    var noOfCores = "4"
    if (args.length > 3) {
      inputPath = args(1)
      noOfCores = args(3)
    }

    val spark = SparkSession
      .builder()
      .appName("INDDiscovery")
      .master("local[" + noOfCores + "]")
      .config("spark.sql.shuffle.partitions", "5")
      .getOrCreate()
    import spark.implicits._

    // Load data from input folder
    val dir: File = new File(inputPath)
    val files = dir.listFiles.filter(_.isFile).toList.map(f => f.toString)
    var dfsBuffer = new ListBuffer[DataFrame]()
    var rddBuffer = new ListBuffer[RDD[(Any, Set[String])]]
    for (i <- files.indices) {
      dfsBuffer += spark.read
        .option("inferSchema", "true")
        .option("header", "true")
        .option("sep", ";")
        .csv(files(i))
    }
    val dfs = dfsBuffer.toList
    dfs.foreach(df => df.columns.foreach(col =>
      rddBuffer += df.select(col).dropDuplicates().rdd.map(r => (r(0), Set[String](col)))
    ))
    val rdds = rddBuffer.toList

    // Reduce toward valid INDs
    val attrSets = rdds.reduce(_ union _).reduceByKey(_ ++ _).values.distinct()
    val INDCandidates = attrSets.flatMap(set => set.map(attr => (attr, set - attr)))
    val validINDs = INDCandidates.reduceByKey(_ intersect _).filter(_._2.nonEmpty)
    val INDStrings = validINDs.map { case (dep, ref) => dep.toString + " < " + ref.mkString(", ") }

    // Output to console
    INDStrings.foreach{println}
  }
}