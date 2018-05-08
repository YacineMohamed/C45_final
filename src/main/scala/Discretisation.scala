import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Discretisation {


  def discretisation(sc:SparkContext,pos:String,rdd:RDD[(String,String)]):Double={
    val categorieDeClasse = Calculs.categorieDeClasse(rdd)

    val total = categorieDeClasse
      .map{case(x,y) => y}.sum()

    val entropieEnsemble = Calculs.entropieEnsemble(categorieDeClasse,total)


    val ensemble = rdd
      .map{case(x,y) => y}
      .flatMap(x=> x.split(","))

    val valeurs = ensemble
      .map(x => x.split("::")(1).split("_")(0))
      .distinct()
      .map(x=> (1,x))
      .reduceByKey(_+","+_)
      .map{case(x,y) => y}
      .map(x => creeInterval(x.split(",").map(_.toDouble).sorted))
      .flatMap(x=>x.split(","))
      .map(x=>x.toDouble)


    var listRapport:RDD[(Double,Double)] = sc.emptyRDD


    val listInterval = sc.broadcast(valeurs.collect())



    listInterval.value.foreach(x=> {
      val ensembleDiscret = ensemble
        .map(k => (k.split("::")(1).split("_")(0).toDouble, k.split("_")(1)))
        .map{case(k,v) =>
          if(k>x) (">"+x+"_"+v)
          else
            ("<"+x+"_"+v)
        }
        .map(x => (pos,x)).reduceByKey(_+","+_)


     val vals =ensembleDiscret.map(x=> x._2)
        .flatMap(x=> x.split(","))
        .map(x=> (x.split("_")(0),x.split("_")(1)))
        .reduceByKey(_+","+_)

   val entropie = Calculs.entropieAttribut(vals)
      val gain = Calculs.gain(entropieEnsemble,entropie,total)


      listRapport = listRapport ++ sc.parallelize(Map(x-> gain).toSeq)

    }
    )

    val posMax = listRapport.reduce((x,y) => if(x._2>y._2) x else y)._1


    return posMax
  }

  def creeInterval(array: Array[Double]): String={
    var resultat=""
    for(i<-0 to array.size-2){
      if(resultat.equals("")){
        resultat = (array(i) + array(i+1))/2+""
      }else{
        resultat= resultat+","+(array(i) + array(i+1))/2
      }
    }

    return resultat
  }


}
