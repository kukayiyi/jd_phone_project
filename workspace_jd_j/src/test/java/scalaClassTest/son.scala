package scalaClassTest

class son extends father("111") {
  def this(chao:String){
    this
    this.body = chao
    println("son constructor" + chao)
  }

}

object mainObject{
  def main(args: Array[String]): Unit = {
    val s = new son("fuck")

  }
}
