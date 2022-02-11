import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

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
    spark.sql("SELECT SUM(Consumers) AS TotalNumConsumers FROM Bev_ConsCountB WHERE Branch = 'Branch2'").show()
    spark.sql("SELECT SUM(Consumers) AS TotalNumConsumers FROM Bev_ConsCountC WHERE Branch = 'Branch2'").show()*/

    //Scenario2

    /*spark.sql("select Drink AS Most_Cons_Bev, sum(Consumers) AS SumDrinkBev from Bev_ConsCountA where Branch = 'Branch1' " +
      "group by Drink order by sum(Consumers) DESC").show(1)
    spark.sql("select Drink AS Most_Cons_Bev, sum(Consumers) AS SumDrinkBev from Bev_ConsCountB where Branch = 'Branch1' " +
      "group by Drink order by sum(Consumers) DESC").show(1)
    spark.sql("select Drink AS Most_Cons_Bev, sum(Consumers) AS SumDrinkBev from Bev_ConsCountC where Branch = 'Branch1' " +
      "group by Drink order by sum(Consumers) DESC").show(1)*/

    /*spark.sql("select Drink AS Most_Cons_Bev, sum(Consumers) AS SumDrinkBev from Bev_ConsCountA where Branch = 'Branch2' " +
      "group by Drink order by sum(Consumers) ").show(1)*/
    /*spark.sql("select Drink AS Most_Cons_Bev, sum(Consumers) AS SumDrinkBev from Bev_ConsCountB where Branch = 'Branch2' " +
      "group by Drink order by sum(Consumers) ").show(1)
    spark.sql("select Drink AS Most_Cons_Bev, sum(Consumers) AS SumDrinkBev from Bev_ConsCountC where Branch = 'Branch2' " +
      "group by Drink order by sum(Consumers) ").show(1)*/

    /*spark.sql("select Drink AS Avg_Drink, sum(Consumers) AS AVG from Bev_ConsCountC where Branch " +
     "= 'Branch2' group by Drink order by sum(Consumers)").show()
*/
    /*spark.sql("select distinct Drink As AvgDrink from Bev_ConsCountA where Drink = 'Mild_Lite'").show()
    spark.sql("select distinct Drink As AvgDrink from Bev_ConsCountB where Drink = 'MED_Lite'").show()
    spark.sql("select distinct Drink As AvgDrink from Bev_ConsCountC where Drink = 'ICY_LATTE'").show()*/



    /*val x = spark.sql("select count(Drink) from Bev_ConsCountC where Branch = 'Branch2'").show()
    val y = spark.sql("select sum(Consumers) from Bev_ConsCountC where Branch = 'Branch2'").show()
    val z = 47733 / 95
    println(z)*/

    // Scenario 3
    /*spark.sql("select Drink from Bev_BranchA where Branch = 'Branch10' or Branch = 'Branch8' or" +
      " Branch = 'Branch1'").show()
    spark.sql("select Drink from Bev_BranchB where Branch = 'Branch10' or Branch = 'Branch8' or" +
      " Branch = 'Branch1'").show()
    spark.sql("select Drink from Bev_BranchC where Branch = 'Branch10' or Branch = 'Branch8' or" +
      " Branch = 'Branch1'").show()*/

    /*spark.sql("select Drink from Bev_BranchA where Branch = 'Branch4' or Branch = 'Branch7'").show()
    spark.sql("select Drink from Bev_BranchB where Branch = 'Branch4' or Branch = 'Branch7'").show()
    spark.sql("select Drink from Bev_BranchC where Branch = 'Branch4' or Branch = 'Branch7'").show()*/

    // Scenario 4
    //spark.sql("drop table if exists BranchPart")
    /*spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    spark.sql("create table if not exists BranchPart(Branch String) partitioned by(Drink String)")
    spark.sql("insert overwrite table BranchPart partition(Drink) select Branch, Drink from Bev_BranchA")
    spark.sql("select * from BranchPart").show()*/

    //some alternate partition code testing
    /*val df = spark.sql("SELECT Drink, Branch FROM Bev_ConsCountA")
    val df2 = df.repartition(9, col("Branch"))
    df2.show()
    println(df2.rdd.getNumPartitions)*/



    // Scenario 5
    /*val userRes = readLine("Would you like to leave a note/Comment")
    spark.sql("alter table Bev_BranchA set tblproperties('Notes' = '"+userRes+ "')")
    spark.sql("show tblproperties Bev_BranchA").show() */

  //spark.sql("create table Bev_ConsCountAB(Drink String ,Consumers Int, Branch String) row format delimited fields terminated by ','");
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConsCountA.csv' INTO TABLE Bev_ConsCountAB")
    /*spark.sql("select Drink AS Least_Cons, sum(Consumers) AS SumDrinkBev from Bev_ConsCountAB where Branch = 'Branch2' " +
      "group by Drink order by sum(Consumers) ").show()*/


    // Scenario 6
  /*  spark.sql("select Drink AS Least_Sold_Drink, sum(Consumers) AS SumDrinkBev from Bev_ConsCountA " +
      "group by Drink order by sum(Consumers) ").show()
    spark.sql("select Drink AS Least_Sold_Drink, sum(Consumers) AS SumDrinkBev from Bev_ConsCountB " +
      "group by Drink order by sum(Consumers) ").show()
    spark.sql("select Drink AS Least_Sold_Drink, sum(Consumers) AS SumDrinkBev from Bev_ConsCountC " +
      "group by Drink order by sum(Consumers) ").show()
*/











    

   //println("created spark session")

    var loggedIn = true
    var loggedIn1 = true
    var loggedIn2 = true
    var loggedIn3 = true
    var loggedIn5 = true
    while(loggedIn){
      println("Welcome, please press a menu option:")
      println("Press:\n 1. For scenario 1\n 2. For scenario 2\n 3. For scenario 3\n 4. For scenario 4" +
        "\n 5. For scenario 5\n 6. For scenario 6\n 7. To exit ")
      val userResponse = readLine().toInt
      userResponse match {
        case 1 =>
          while(loggedIn1) {
            val res = readLine("Press 1:" +
              " For what is the total number of consumers for Branch1?\n" +
              "Press 2: For What is the number of consumers for the Branch2?\n" +
              "Press 3: To go back ").toInt
            res match {
              case 1 => spark.sql("SELECT SUM(Consumers) AS TotalNumConsumers FROM Bev_ConsCountA WHERE Branch = " +
                "'Branch1'").show()
                spark.sql("SELECT SUM(Consumers) AS TotalNumConsumers FROM Bev_ConsCountB WHERE Branch = 'Branch1'").show()
                spark.sql("SELECT SUM(Consumers) AS TotalNumConsumers FROM Bev_ConsCountC WHERE Branch = 'Branch1'").show()

              case 2 => spark.sql("SELECT SUM(Consumers) AS TotalNumConsumers FROM Bev_ConsCountA WHERE Branch = " +
                "'Branch2'").show()
                spark.sql("SELECT SUM(Consumers) AS TotalNumConsumers FROM Bev_ConsCountA WHERE Branch = 'Branch2'").show()
                spark.sql("SELECT SUM(Consumers) AS TotalNumConsumers FROM Bev_ConsCountA WHERE Branch = 'Branch2'").show()

              case 3 => loggedIn1 = false

              case _ => println("Not a valid input ")
            }
          }
        case 2 =>
          while(loggedIn2) {
            val res = readLine("Press 1:" +
              " What is the most consumed beverage on Branch1\n" +
              "Press 2: What is the least consumed beverage on Branch2\n" +
              "Press 3: What is the Average consumed beverage of Branch2\n" +
              "Press 4: To go back ").toInt
            res match {
              case 1 => spark.sql("select Drink AS Most_Cons_Bev, sum(Consumers) AS SumDrink from Bev_ConsCountA where Branch = 'Branch1' " +
                "group by Drink order by sum(Consumers) DESC").show(1)
                spark.sql("select Drink AS Most_Cons_Bev, sum(Consumers) AS SumDrink from Bev_ConsCountB where Branch = 'Branch1' " +
                  "group by Drink order by sum(Consumers) DESC").show(1)
                spark.sql("select Drink AS Most_Cons_Bev, sum(Consumers) AS SumDrink from Bev_ConsCountC where Branch = 'Branch1' " +
                  "group by Drink order by sum(Consumers) DESC").show(1)

              case 2 => spark.sql("select Drink AS Least_Cons_Bev, sum(Consumers) AS SumDrink from Bev_ConsCountA where Branch = 'Branch2' " +
                "group by Drink order by sum(Consumers) ").show(1)
                spark.sql("select Drink AS Least_Cons_Bev, sum(Consumers) AS SumDrink from Bev_ConsCountB where Branch = 'Branch2' " +
                  "group by Drink order by sum(Consumers) ").show(1)
                spark.sql("select Drink AS Least_Cons_Bev, sum(Consumers) AS SumDrink from Bev_ConsCountC where Branch = 'Branch2' " +
                  "group by Drink order by sum(Consumers) ").show(1)

              case 3 => spark.sql("select distinct Drink As AvgDrink from Bev_ConsCountA where Drink = 'Mild_Lite'").show()
              spark.sql("select distinct Drink As AvgDrink from Bev_ConsCountB where Drink = 'MED_Lite'").show()
              spark.sql("select distinct Drink As AvgDrink from Bev_ConsCountC where Drink = 'ICY_LATTE'").show()

              case 4 => loggedIn2 = false

              case _ => println("Not a valid input!")
            }
          }
        case 3 =>
          while(loggedIn3) {
            val res = readLine("Press 1: " +
              "What are the beverages available on Branch10, Branch8, and Branch1?\n" +
              "Press 2: What are the comman beverages available in Branch4,Branch7?\n" +
              "Press 3: To go back").toInt
            res match {
              case 1 => spark.sql("select Drink from Bev_BranchA where Branch = 'Branch10' or Branch = 'Branch8' or" +
                " Branch = 'Branch1'").show()
                spark.sql("select Drink from Bev_BranchB where Branch = 'Branch10' or Branch = 'Branch8' or" +
                  " Branch = 'Branch1'").show()
                spark.sql("select Drink from Bev_BranchC where Branch = 'Branch10' or Branch = 'Branch8' or" +
                  " Branch = 'Branch1'").show()

              case 2 => spark.sql("select Drink from Bev_BranchA where Branch = 'Branch4' or Branch = 'Branch7'").show()
                spark.sql("select Drink from Bev_BranchB where Branch = 'Branch4' or Branch = 'Branch7'").show()
                spark.sql("select Drink from Bev_BranchC where Branch = 'Branch4' or Branch = 'Branch7'").show()

              case 3 => loggedIn3 = false

              case _ =>
            }
          }
        case 4 => val df = spark.sql("SELECT Drink, Branch FROM Bev_ConsCountA")
          val df2 = df.repartition(9, col("Branch"))
          df2.show()
          println("The numbers of partitions are: ")
          println(df2.rdd.getNumPartitions)
        case 5 =>
          while(loggedIn5){
            val res = readLine("Press 1: To add a note/comment\n" +
              "Press 2: to remove a row from a table\n " +
              "Press 3: To go back").toInt
            res match {
              case 1 => val usersNote = readLine("Please enter a Note/Comment")
                spark.sql("alter table Bev_BranchA set tblproperties('Notes' = '"+usersNote+ "')")
                spark.sql("show tblproperties Bev_BranchA").show()

              case 2=> spark.sql("select Drink AS Least_Cons, sum(Consumers) AS SumDrinkBev from Bev_ConsCountAB where Branch = 'Branch2' " +
                "group by Drink order by sum(Consumers) ").show()

              case 3=> loggedIn5 = false
            }
          }
        case 6 => println("Specials of the day:\n Med_Coffee, Small_Lite, and Mild_Lite :) Available at all stores! ")
          spark.sql("select Drink AS Least_Sold_Drink, sum(Consumers) AS SumDrinkBev from Bev_ConsCountA " +
            "group by Drink order by sum(Consumers) ").show()
        case 7 => loggedIn = false
        println("Good-bye!")

        case _ => println("Invalided input")
      }
    }

  }

}
