/**
 * Created by hastimal on 9/22/2015.
 */
object StringUtils {

  def onlyWords(text: String) : String = {
    text.split(" ").filter(_.matches("^[a-zA-Z0-9 ]+$")).fold("")((a,b) => a + " " + b).trim
  }

}