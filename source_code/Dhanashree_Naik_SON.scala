import org.apache.spark.SparkContext
import org.apache.spark.RangePartitioner
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import java.io._

object Dhanashree_Naik_SON {
	def make_map(data:String): Map[String, String] = {
		
		 val array = data.split("::")
		 var dataMap = Map[String, String]()
		 if (array(1)=="M"){
		 dataMap = Map(array(0) -> array(1))
		 }
		return dataMap
		}
	def make_map1(data:String): Map[String, String] = {
		
		 val array = data.split("::")
		 var dataMap = Map[String, String]()
		 if (array(1)=="F"){
		 dataMap = Map(array(0) -> array(1))
		 }
		return dataMap
		}
  
	def make_map2(data:String): Map[String, String] = {
		
		 val array = data.split("::")
		 var dataMap = Map[String, String]()
		 if (array(1)=="M"){
		 dataMap = Map(array(0) -> array(1))
		 }
		return dataMap
		}
	
	def createDataMap(data:String): Map[String, String] = {
		 val array = data.split("::")
		 val dataMap = Map[String, String](
		 ( array(0) -> array(1)) 
		 )
		return dataMap
		}
	
	def createDataMap3(movieId:Int, user:Int ): Map[Int, Int] = {
		 //Creating the Map of values
		 val dataMap = Map[Int, Int](
		 ( 
		    ( user ->  movieId )
		 ))
		return dataMap
		}
	
	def create_son_map1(data:Set[Int],num:Int): Map[Set[Int], Int] = {
		 
		 val dataMap = Map[Set[Int], Int](
		 ( data -> 1) 
		 )
		return dataMap
		}
	
	def runapriory(map:Iterator[(Int,Set[Int])], support:Float) : scala.collection.mutable.Map[Set[Int],Int]  ={
	  var Candidate = new scala.collection.mutable.HashMap[Set[Int],Int]()
	  var final_list = new scala.collection.mutable.ListBuffer[Set[Int]]()
	  var final2 =  scala.collection.mutable.Map[Set[Int],Int]()
	  var freq1 = scala.collection.mutable.Set[Int]()
      var freq2 = new scala.collection.mutable.ListBuffer[Set[Int]]()
      var values = new scala.collection.mutable.ListBuffer[Set[Int]]()
      for (i<-map){
        values += i._2 
      }
        
	  for (f <- values )
	  {	  for (i <- f) {			  
			  var x = Candidate.getOrElse(Set(i), 0) + 1
			  Candidate += Set(i) -> x }}  	  

	  	  
	  for (m <- Candidate) {
	    if (m._2 > support){ 
	      freq2 += m._1 
	    }
	  }
	  
	  for (x<-freq2){
	  for (i<-x){
	    freq1 += i
	  }
	  }
	  
	  final_list ++= freq2	
	  
	 var count = 2
	  while (!Candidate.isEmpty)
	  { 
	    var flagged = scala.collection.mutable.Set[Set[Int]]()
	    //var count = 0
	    //count = freq2(0).size +1
	    //count = freq2(0).size
	    Candidate = Candidate.empty
	    var temp = new scala.collection.mutable.ListBuffer[Set[Int]]()

	    for (i <- 0 until freq2.size) {
	      
	      var diff = freq1.diff(freq2(i)).toList
	      for (j <- 0 until diff.size) {
	        
	        
	        var temp2 = freq2(i).union(Set(diff(j)))
	        if (!(flagged.contains(temp2))){
	        flagged = flagged.union(Set(temp2))
	        val combines = temp2.toList.combinations(count-1).toList.toSet	  
			var indicator = true
			  
			for (m <-combines){		    
			   if (!(freq2.contains(m.toSet)))
			    {
			    indicator = false
			    }}
			  if (indicator){
			  	  for (x<-values)
				  {
				    if (x.intersect(temp2).equals(temp2))
				    {
				      val count_202: Int = Candidate.getOrElse(temp2,0) +1
					  Candidate.update(temp2, count_202)  
				    }}}  
		        }}}
	  freq2 = new scala.collection.mutable.ListBuffer[Set[Int]]()
	  for (t <- Candidate) {
	    if (t._2 > support){ 
	      freq2 += t._1 }}
	  
	  if (!freq2.isEmpty){
	  final_list ++= freq2}
	  count += 1
	  }
	  for (i<-final_list){
	    final2.update(i, 1)
	  
	  }
	  return final2

	 }
	
	def find_freq(map:Iterator[(Int,Set[Int])], new_some:Array[Set[Int]]) : scala.collection.mutable.Map[Set[Int],Int] ={
	  
	  var final_ans = scala.collection.mutable.Map[Set[Int],Int]()
	  var values = new scala.collection.mutable.ListBuffer[Set[Int]]()
      for (i<-map){
        values += i._2 
      }
	  
	  for (j<-values){
	    
	  for (i<-new_some){
	    if (j.intersect(i).equals(i)){
	      val count_111: Int = final_ans.getOrElse(i,0) +1
		  final_ans.update(i, count_111) 
	    }
	  }
	  
	  }
	  return final_ans
	}
	
	def sort_now(first:String, second:String) = {first < second}
	
	def main (args: Array[String]){
	  
		
	    val start = System.nanoTime
		val usersFile = args(1)
	    val ratingFile = args(2)
	    val conf = new SparkConf().setAppName("Sample Application").setMaster("local[*]")
	    val sc = new SparkContext(conf)
	    val usersFileLines = sc.textFile(usersFile , 4).cache()
	    val ratingFileLines = sc.textFile(ratingFile , 4).cache()
	    val cs = args(0).toInt
	    val support =args(3).toInt
	    
	    if (cs==1){
	    val output = new PrintWriter(new File("Dhanashree_Naik_SON.case"+cs+"_"+support+".txt" ))

		val user_words = usersFileLines.flatMap(f=> make_map(f))
		//user_words.foreach(f=>println(f))		
		val movie_words = ratingFileLines.flatMap(x=>createDataMap(x))
		val users_to_movies =  movie_words.join(user_words )
		
		val finalMap = users_to_movies.flatMap(f=>createDataMap3( f._2 ._1.toInt ,f._1.toInt ))
		val group =finalMap.aggregateByKey(scala.collection.mutable.Set.empty[Int])(
        (numList, num) => {numList += num; numList},
         (numList1, numList2) => {numList1 ++=(numList2); numList1}).mapValues(_.toSet)
	 
		val new_map = group.mapPartitions((iterator)=> runapriory(iterator,support/group.getNumPartitions).iterator)		
		//println(new_map.count)
		
		val something = new_map.reduceByKey(_+_)		
		val new_some = something.keys.distinct.collect
		//something.foreach(f=>println(f))
		//println(something.count)
		val phase2 = group.mapPartitions((iterator)=> find_freq(iterator,new_some).iterator)
        val something2 = phase2.reduceByKey(_+_)
        var final_case_1 = List[List[Int]]()
        for (i<-something2.collect){
          if(i._2 >= support){
            final_case_1 = i._1.toList.sortWith(_ < _) :: final_case_1    
          } 
        }
		
	    var final_cout = 1
	    var final_List = scala.collection.mutable.HashMap[Int,List[String]]()
        for (j<-final_case_1.sortBy(f=>f.sum).sortBy(f=>f(0)).sortBy(f=>f.size)){
          if (j.size==final_cout){
            var final_temp = ""
              for (y<-j){
               for (z <-0 to (5-y.toString.length())){
                 final_temp += "0"  
               }
               final_temp += y.toString +","
              }
              final_temp = final_temp.dropRight(1)
	          var name = final_List.getOrElse(final_cout,List[String]())
	          name = final_temp :: name
	          final_List.update(final_cout, name)
          }
          else if(j.size==final_cout+1){	        
	           var final_temp = ""
              for (y<-j){
               for (z <-0 to (5-y.toString.length())){
                 final_temp += "0"  
               }
               final_temp += y.toString +","
              }
              final_temp = final_temp.dropRight(1)
	           var name = final_List.getOrElse(final_cout+1,List[String]()) 
	          name = final_temp :: name
	          final_List.update(final_cout+1, name)
	          final_cout+=1}}
	    
	    var FINAL_lIST = scala.collection.mutable.HashMap[Int,List[String]]() 
	    		for (i <- final_List.keys) {
	    			var temp444 = List[String]()
	    					temp444 = final_List(i)
	    					FINAL_lIST.update(i, temp444.sortWith(sort_now))
    	}
         
   
        
        var ans = ""
        
         var final_temp1 = ""
          for (j <- FINAL_lIST(1)){
            
            var new_temp1 = "("
            
              new_temp1 += j.toInt.toString + ","+ " "
            
            new_temp1 = new_temp1.dropRight(2) + ")"+ "," + " "
            final_temp1 = final_temp1 + new_temp1 
          }
          final_temp1=final_temp1.dropRight(2)
          output.write(final_temp1)
          
          
        
        for (i <- 2 to FINAL_lIST.size ){
          output.write("\n")
          var final_temp = ""
          for (j <- FINAL_lIST(i)){
            var temp = j.split(",")
            var new_temp = "("
            for ( t<- temp){
              new_temp += t.toInt.toString + ","+ " "
            }
            new_temp = new_temp.dropRight(2) + ")"+ "," + " "
            final_temp = final_temp + new_temp 
          }
          final_temp=final_temp.dropRight(2)
          output.write(final_temp)
        }
	    output.close()
        //println(final_case_1.length)
		val end = (System.nanoTime - start) / 1e9d
		println (end)}
	    
	    if (cs==2){
	    
	    val output = new PrintWriter(new File("Dhanashree_Naik_SON.case"+cs+"_"+support+".txt" ))

		 val user_words = usersFileLines.flatMap(f=> make_map1(f))	
	    	
	    val movie_words = ratingFileLines.flatMap(x=>createDataMap(x))	     
	    val users_to_movies =  movie_words.join(user_words )	       
	    val finalMap = users_to_movies.flatMap(f=>createDataMap3(  f._1.toInt , f._2 ._1.toInt ))
	    
		val group =finalMap.aggregateByKey(scala.collection.mutable.Set.empty[Int])(
        (numList, num) => {numList += num; numList},
         (numList1, numList2) => {numList1 ++=(numList2); numList1}).mapValues(_.toSet)
	 
		val new_map = group.mapPartitions((iterator)=> runapriory(iterator,support/group.getNumPartitions).iterator)		
		//println(new_map.count)
		
		val something = new_map.reduceByKey(_+_)		
		val new_some = something.keys.distinct.collect
		//something.foreach(f=>println(f))
		//println(something.count)
		val phase2 = group.mapPartitions((iterator)=> find_freq(iterator,new_some).iterator)
        val something2 = phase2.reduceByKey(_+_)
        var final_case_1 = List[List[Int]]()
        for (i<-something2.collect){
          if(i._2 >= support){
            final_case_1 = i._1.toList.sortWith(_ < _) :: final_case_1    
          } 
        }
		
	    var final_cout = 1
	    var final_List = scala.collection.mutable.HashMap[Int,List[String]]()
        for (j<-final_case_1.sortBy(f=>f.sum).sortBy(f=>f(0)).sortBy(f=>f.size)){
          if (j.size==final_cout){
            var final_temp = ""
              for (y<-j){
               for (z <-0 to (5-y.toString.length())){
                 final_temp += "0"  
               }
               final_temp += y.toString +","
              }
              final_temp = final_temp.dropRight(1)
	          var name = final_List.getOrElse(final_cout,List[String]())
	          name = final_temp :: name
	          final_List.update(final_cout, name)
          }
          else if(j.size==final_cout+1){	        
	           var final_temp = ""
              for (y<-j){
               for (z <-0 to (5-y.toString.length())){
                 final_temp += "0"  
               }
               final_temp += y.toString +","
              }
              final_temp = final_temp.dropRight(1)
	           var name = final_List.getOrElse(final_cout+1,List[String]()) 
	          name = final_temp :: name
	          final_List.update(final_cout+1, name)
	          final_cout+=1}}
	    
	    var FINAL_lIST = scala.collection.mutable.HashMap[Int,List[String]]() 
	    		for (i <- final_List.keys) {
	    			var temp444 = List[String]()
	    					temp444 = final_List(i)
	    					FINAL_lIST.update(i, temp444.sortWith(sort_now))
    	}

         var final_temp1 = ""
          for (j <- FINAL_lIST(1)){
            
            var new_temp1 = "("
            
              new_temp1 += j.toInt.toString + ","+ " "
            
            new_temp1 = new_temp1.dropRight(2) + ")"+ "," + " "
            final_temp1 = final_temp1 + new_temp1 
          }
          final_temp1=final_temp1.dropRight(2)
          output.write(final_temp1)
          
          
        
        for (i <- 2 to FINAL_lIST.size ){
          output.write("\n")
          var final_temp = ""
          for (j <- FINAL_lIST(i)){
            var temp = j.split(",")
            var new_temp = "("
            for ( t<- temp){
              new_temp += t.toInt.toString + ","+ " "
            }
            new_temp = new_temp.dropRight(2) + ")"+ "," + " "
            final_temp = final_temp + new_temp 
          }
          final_temp=final_temp.dropRight(2)
          output.write(final_temp)
        }
	    output.close()
        //println(final_case_1.length)
		val end = (System.nanoTime - start) / 1e9d
		println (end)}
  }

}
