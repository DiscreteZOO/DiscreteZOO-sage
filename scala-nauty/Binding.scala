/**
 * Created by katja on 04/11/15.
 */
class Binding {
  @native
  def foo(a: Int, b: Int, c: Int, s: String): String
  System.loadLibrary("Binding")
  def carrot: Unit = println("Howdy Carrot!")
}
