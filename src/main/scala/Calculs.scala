import Construction.calculEntropie
import Traitement.Calcul.log2
import org.apache.spark.rdd.RDD

object Calculs {

  def gain(entropieEnsemble:Double, entropieAttribut:RDD[(Int,Double)],total:Double):Double={


    val gain = entropieEnsemble-entropieAttribut
      .map{case(k,v) => (v*k)/total}
      .sum()

    return gain

  }



  def categorieDeClasse(rdd:RDD[(String,String)]): RDD[(String,Int)] ={

    val categorieDeClasse:RDD[(String,Int)] = rdd.map{case(x,y) => y}
      .flatMap(x=> x.split(","))
      .map(x=> (x.split("::")(0) , x.split("::")(1).split("_")(1)))
      .distinct()
      .map(x=> (x._2,1)).reduceByKey(_+_).cache()

    return categorieDeClasse
  }

  def entropieEnsemble(categorieDeClasse:RDD[(String,Int)],total:Double): Double ={
    val entropieEnsemble =
      categorieDeClasse.map{case(x,y) =>
        (-(y.toDouble / total) * log2(y.toDouble / total))
      }
        .sum()
    return entropieEnsemble
  }

  def entropieAttribut(valeurAttribut:RDD[(String,String)]): RDD[(Int,Double)] ={


    val entropieAttribut = valeurAttribut
      .map{case(k,v) => (v.split(",").size,calculEntropie(v.split(",")))}

return entropieAttribut
  }
}
