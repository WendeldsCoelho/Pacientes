//import org.apache.spark.sql.DataFrame
//import org.apache.spark.sql.functions._
//import plotly._
//import plotly.element._
//import plotly.layout._
//import plotly.Plotly
//
//object questao8 {
//  def run(dfRenomeado: DataFrame): Unit = {
//    dfRenomeado
//      .agg(round(avg("duracao"), 2).as("duracaoMedia"))  // Calcula a média e arredonda
//      .select("duracaoMedia")  // Seleciona apenas a coluna de duração média
//    //  .show(false)  // Exibe o valor sem truncar
//
//    val duracaoMedia = dfRenomeado
//      .agg(round(avg("duracao"), 2).as("duracaoMedia"))
//      .collect()(0).getDouble(0)
//
//    val layout = Layout(
//      title = "Duração Média dos Tratamentos"
//    )
//
//    val grafico = Indicator(
//      mode = IndicatorMode.Number,
//      value = duracaoMedia,
//      title = "Duração Média (dias)"
//    )
//
//    Plotly.plot(
//      path = "duracao_media.html",
//      traces = Seq(grafico),
//      layout = layout
//    )
//
//  }
//}