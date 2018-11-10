import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.lang.Math._;

object KMeans {

  
    type Point = (Double, Double);
  var centroids: Array[Point] = Array[Point]()
  var finalpoint : Array[Point] = Array[Point]()
  var arr2 : Array[(Point, Point)] = Array[(Point, Point)]()

  //Finding the nearest Centroid

  def centroid_locator(arr2 : Array[Point], p : Point) : Point = 
  {
    
    var minimum_distance : Double = 99999;
    var euclidean : Double = 99999;
    var index : Int = 99999;
    var count : Int = 0;

    for(x <- arr2)
    {
      euclidean = Math.sqrt(((x._1-p._1)*(x._1 - p._1)) + ((x._2-p._2)*(x._2-p._2)));
      if(euclidean < minimum_distance)
      {
        minimum_distance = euclidean; 
        index = count;
      }
      count = count + 1
    }
    
    return (arr2(index)._1, arr2(index)._2)
  }

  
def reduce1(cent : Point , points : Seq[Point]) : Point = 
  {
    
    var count : Int = 0;
    var sval : Double = 0;
    var yval : Double = 0;

    for(i <- points)
    {
      count= count+1 ;  
      sval += i._1 ;
      yval += i._2 ;
    }
    return (sval/count, yval/count);


  }

  def main(args: Array[ String ]) {
    /* ... */
    val conf = new SparkConf().setAppName("Kmeans");
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf);
    
/* read initial centroids from centroids.txt */
    centroids = sc.textFile(args(1)).collect.map( line => { val a = line.split(",")
                                                    (a(0).toDouble,a(1).toDouble)})
    finalpoint  = sc.textFile(args(0)).collect.map( x => {val a = x.split(",") 
                                              (a(0).toDouble, a(1).toDouble)});
 /*find new centroids using KMeans */
    for ( i <- 1 to 5 )
    {
    arr2 = finalpoint.map(d => {((centroid_locator(centroids, d), (d._1, d._2)))});
    var d = sc.parallelize(arr2)
    var mapper = d.mapValues(value => (value,1))
    var new_centroids = mapper.reduceByKey{ case ((x1, x2), (y1, y2)) => (((x1._1 + y1._1),(x1._2 + y1._2)), (x2 + y2)) }             
                 .mapValues{
                  case (x, y) => (x._1/y, x._2/y) }
                 .collect
    centroids = new_centroids.map(a => (a._2))
    }
    
    centroids.foreach(println)
  }
}
