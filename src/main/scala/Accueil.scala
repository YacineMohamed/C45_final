import java.util.Calendar

import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer
import scala.reflect.io.Path

object Accueil {

  val sc = Initialisation.Spark.sc

  def main(args: Array[String]): Unit = {

    val fichierEntee =  args(0)

    val entete="0"
    val ID="0"
    val separateur=","
    val attributContinue=""
    val ensembleStructure = Preparation.structureInitiale(sc,fichierEntee,separateur,entete,ID,attributContinue)


    val debut = ""+Calendar.getInstance().getTime.getMinutes+":"+Calendar.getInstance().getTime.getSeconds



    val arbre = Construction.chercherAttribut(sc,"",ensembleStructure)

    enregistreArbre(debut,arbre, args(1))


  }

  def enregistreArbre(debut:String,res:RDD[String],output: String): Unit = {
    if (scala.reflect.io.File(scala.reflect.io.Path(output)).exists) {
      val jj: Path = Path(output)
      jj.deleteRecursively()
      res.coalesce(1,true).saveAsTextFile(output)
    } else {
      res.coalesce(1,true).saveAsTextFile(output)
    }
    println("CREATION ... [DEBUT] " +debut)
    println("CREATION ... [FIN] " +Calendar.getInstance().getTime.getMinutes+":"+Calendar.getInstance().getTime.getSeconds)
  }

  def structurerEnsemble(ligne:Long,array: Array[String]): String = {
    var result =""
    for(i<-0 to array.size-2){
      if(result.equals("")){
        result = ligne+"::"+array(i)+"_"+array(array.size-1)+"#"+i
      }else{
        result = result+","+ligne+"::"+array(i)+"_"+array(array.size-1)+"#"+i
      }
    }
    return result
  }
}