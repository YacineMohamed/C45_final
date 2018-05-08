import Traitement.Calcul.log2
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Construction {
var sc = Initialisation.Spark.sc
var resultat=""
var rddRes:RDD[String]=sc.emptyRDD

  def chercherAttribut(sc:SparkContext, noeud:String, rdd:RDD[(String,String)]): RDD[String] ={
    //rdd.foreach(x=> println(x._1+" ... "+x._2))
if(rdd.count()==1){
println("khrakhra !!! ")
  val compte  = rdd.map(x=> x._2)
    .flatMap(x=> x.split(",")).map(x=> (x.split("_")(1),1))
    .reduceByKey(_+_)
    .reduce((x,y) => if(x._2> y._2) x else y)._1
//  compte.foreach(x=> println(x._1+" "+x._2))

  rddRes = rddRes ++ sc.parallelize(Seq(noeud+"="+compte))

}else{


  val categorieDeClasse = Calculs.categorieDeClasse(rdd)

  if(categorieDeClasse.count()>1){

    val total = categorieDeClasse
      .map{case(x,y) => y}.sum()

    /***________________________________*/
    val entropieEnsemble =Calculs.entropieEnsemble(categorieDeClasse,total)


    val positionAttributs = sc.broadcast(rdd.map{case(x,y) => x}.collect())



    var listRapport:RDD[(String,Double)]=sc.emptyRDD

    positionAttributs.value.foreach(x => {

      val valeurAttribut =
        rdd.filter(k=> (k._1.equals(x)))
          .map(k => k._2)
          .flatMap(k=> k.split(","))
          .map(k => (k.split("_")(0).split("::")(1) , k.split("_")(1)))
          .reduceByKey(_+","+_)

      val infoSplit =
        valeurAttribut.map(x=> (x._2).split(",").size)
          .map(x=> (- (x/total)*log2(x/total)  ))
          .sum()

      val entropieAttribut = Calculs.entropieAttribut(valeurAttribut)

      val gain = Calculs.gain(entropieEnsemble,entropieAttribut,total)
      if(infoSplit==0){
        println("khra "+x)
        rdd.foreach(x=> println(x._1+" "+x._2))

        listRapport = listRapport ++ sc.parallelize(Map(x->(0.0)).toSeq)

      }else{
        listRapport = listRapport ++ sc.parallelize(Map(x->(gain/infoSplit)).toSeq)
      }
    })



    val postMAX = listRapport.reduce((x,y)=> if(x._2> y._2) x else y)._1
    /***  ****************************************************** */

    val nouvelEns = rdd
      .filter { case (x, y) => (!x.equals(postMAX ))}
      .map{case(x,y) => (changerStructure(x,y.split(",")))}
      .flatMap(x=> x.split(","))
      .map(x=> (x.split("::")(0), x.split("::")(1)))


    val valAttDiv = rdd
      .filter { case (x, y) => (x.equals(postMAX)) }



    val valeurLigne =   valAttDiv.map(x=> x._2)
      .flatMap(x=> x.split(","))
      .map(x=> (x.split("::")(1).split("_")(0) , x.split("::")(0)))
      .reduceByKey(_+","+_)

    val broadcastValLigne = sc.broadcast(valeurLigne.collectAsMap())

    broadcastValLigne.value.foreach { case (x, y) =>

      val sousEnsemble = nouvelEns.filter{case(c,v) =>
        Traitement.Calcul.testExiste2(c,y.split(","))}
        .map { case (xx, yy) => (yy.split("#")(1), xx+"::"+yy.split("#")(0)) }
        .reduceByKey(_ + "," + _)

      var nouvNoeud = ""
      if (noeud.equals("")) {
        nouvNoeud = postMAX + "::" + x
      } else {
        nouvNoeud = noeud + "=" + postMAX + "=" + x
      }
      if(sousEnsemble.isEmpty()){

      }else {
        chercherAttribut(sc, nouvNoeud, sousEnsemble)
      }
    }

  }else{


    rddRes = rddRes ++ sc.parallelize(Seq(noeud+"="+categorieDeClasse.first()._1+""))
  }


}


    return rddRes
  }

  def diviserEnsemble(): Unit ={

  }


  def calculEntropie(array: Array[String]): Double ={
    var resultat =""
    var nbOccurence = array.toSeq.groupBy(identity).mapValues(_.size)
    val total = nbOccurence.values.sum

    var entropie=0.0
    for ((k,v) <- nbOccurence)
    {
      entropie += -( v.toDouble / total ) * log2( v.toDouble / total )
    }

    return entropie
  }


  def changerStructure(pos:String,array:Array[String]): String ={
    var resultat=""
    for(i<-0 to array.size-1){
      if(resultat.equals("")){
        resultat = array(i)+"#"+pos
      }else{
        resultat = resultat+","+array(i)+"#"+pos
      }
    }
    return resultat
  }


}
