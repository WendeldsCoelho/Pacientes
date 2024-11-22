import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import plotly._
import plotly.element._
import plotly.layout._
import plotly.Plotly
object questao3 {
  def run(df: DataFrame): Unit = {
    // Filtrar tratamentos com duração superior a 30 dias e calcular duração total
    val tratamentosLongos = df
      .filter(col("duracao") > 30)
      .groupBy(col("tratamento"))
      .agg(sum(col("duracao")).alias("Duração Total (dias)"))

    // Coletar dados para o gráfico
    val dados = tratamentosLongos.collect()
    val tratamentos: Seq[String] = dados.map(_.getString(0))      // Nome do tratamento
    val duracoesTotais: Seq[Double] = dados.map(_.getLong(1).toDouble) // Duração total convertida para Double

    // Criar gráfico de barras
    val grafico = Bar(
      x = tratamentos,    // Tipos de tratamento no eixo X
      y = duracoesTotais  // Duração total no eixo Y
    )

    // Configurar layout do gráfico
    val layout = Layout(
      title = "Proporção de Duração de Tratamentos Longos (> 30 dias)",
      xaxis = Axis().withTitle("Tratamento"),
      yaxis = Axis().withTitle("Duração Total (dias)"),
      margin = Margin(120, 45, 120, 150), // Margens ajustadas
      height = 700, // Aumenta a altura do gráfico
      width = 500,  // Reduz a largura do gráfico
      bargap = 0.2  // Define o espaçamento entre barras para deixá-las mais finas
    )

    // Gerar e salvar o gráfico
    val caminhoArquivo = "tratamentos_longos_proporcao_vertical.html"
    Plotly.plot(
      path = caminhoArquivo,
      traces = Seq(grafico),
      layout = layout,
      config = Config(),
      useCdn = true,
      openInBrowser = true,
      addSuffixIfExists = true
    )

    // Exibir tabela no console
    tratamentosLongos.show(false)

    println(s"Gráfico salvo e aberto no navegador: $caminhoArquivo")
  }
}