import org.apache.spark.sql.SparkSession
import scala.io.StdIn.readLine

object Project1 {

  def main(args: Array[String]): Unit = {
    // create a spark session
    // for Windows
    System.setProperty("hadoop.home.dir", "C:\\winutils")

    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
       spark.sparkContext.setLogLevel("WARN")
    //for partitioning
    //spark.sql("create table if not exists Branch_Partition (Beverage String) partitioned by(Branch String) +
    // row format delimited fields terminated by ','")


    //spark.sql("select b.beverage, sum(c.count) from bev_branch b join bev_count c on b.beverage = c.beverage where branch " +
    //  "= 'Branch2' group by b.beverage order by sum(c.count) asc limit 1").show()
    /*spark.sql("create table Bev_ConsCountC (Drink String, Consumers Int, Branch String) row format delimited fields terminated by ','");
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountC.csv' INTO TABLE Bev_ConsCountC")
    spark.sql("SELECT * FROM Bev_ConsCountC").show()*/

    //Scenario1

    /*spark.sql("SELECT SUM(Consumers) AS TotalNumConsumers FROM Bev_ConsCountA WHERE Branch = 'Branch1'").show()
    spark.sql("SELECT SUM(Consumers) AS TotalNumConsumers FROM Bev_ConsCountB WHERE Branch = 'Branch1'").show()
    spark.sql("SELECT SUM(Consumers) AS TotalNumConsumers FROM Bev_ConsCountC WHERE Branch = 'Branch1'").show()

    spark.sql("SELECT SUM(Consumers) AS TotalNumConsumers FROM Bev_ConsCountA WHERE Branch = 'Branch2'").show()
    spark.sql("SELECT SUM(Consumers) AS TotalNumConsumers FROM Bev_ConsCountA WHERE Branch = 'Branch2'").show()
    spark.sql("SELECT SUM(Consumers) AS TotalNumConsumers FROM Bev_ConsCountA WHERE Branch = 'Branch2'").show()*/

    //Scenario2

    /*spark.sql("select Drink AS Most_Cons_Bev, sum(Consumers) AS SumDrinkBev from Bev_ConsCountA where Branch = 'Branch1' " +
      "group by Drink order by sum(Consumers) DESC").show(1)
    spark.sql("select Drink AS Most_Cons_Bev, sum(Consumers) AS SumDrinkBev from Bev_ConsCountB where Branch = 'Branch1' " +
      "group by Drink order by sum(Consumers) DESC").show(1)
    spark.sql("select Drink AS Most_Cons_Bev, sum(Consumers) AS SumDrinkBev from Bev_ConsCountC where Branch = 'Branch1' " +
      "group by Drink order by sum(Consumers) DESC").show(1)*/

    spark.sql("select Drink AS Most_Cons_Bev, sum(Consumers) AS SumDrinkBev from Bev_ConsCountB where Branch = 'Branch1' " +
      "group by Drink order by sum(Consumers) ").show(1)




   /* println("created spark session")
    println("Welcome, please press a menu option:")
    println("Press:\n 1. For scenario 1\n 2. For scenario 2\n 3. For scenario 3\n 4. For scenario 4" +
      "\n 5. For scenario 5\n 6. For scenario 6")

    val userResponse = readLine().toInt
    userResponse match {
      case 1 => val res = readLine("Press 1:" +
        " For what is the total number of consumers for Branch1?\n" +
        "Press 2: For What is the number of consumers for the Branch2? ").toInt
      res match {
        case 1 => spark.sql("SELECT SUM(Consumers) AS TotalNumConsumers FROM Bev_ConsCountA WHERE Branch = " +
          "'Branch1'").show()
          spark.sql("SELECT SUM(Consumers) AS TotalNumConsumers FROM Bev_ConsCountB WHERE Branch = 'Branch1'").show()
          spark.sql("SELECT SUM(Consumers) AS TotalNumConsumers FROM Bev_ConsCountC WHERE Branch = 'Branch1'").show()

        case 2 => spark.sql("SELECT SUM(Consumers) AS TotalNumConsumers FROM Bev_ConsCountA WHERE Branch = " +
          "'Branch2'").show()
          spark.sql("SELECT SUM(Consumers) AS TotalNumConsumers FROM Bev_ConsCountA WHERE Branch = 'Branch2'").show()
          spark.sql("SELECT SUM(Consumers) AS TotalNumConsumers FROM Bev_ConsCountA WHERE Branch = 'Branch2'").show()

        case _ => println("Not a valid input ")
      }
      case 2 => println("test 2")
      case 3 => println("test 3")
      case 4 => println("test 4")
      case 5 => println("test 5")
      case 6 => println("test 6")
      case _ => println("Invalided input")
    }
*/
  }

}
