package scalaClassTest

class father {
  var body:String = _
  def this(chao:String){
    this
    this.body = chao
    println("father construct" + body)
  }

}
