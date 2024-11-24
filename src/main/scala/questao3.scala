import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import plotly._
import plotly.element._
import plotly.layout._
import plotly.Plotly

object questao3 {
  def run(dfRenomeado: DataFrame): Unit = {
    // Filtra tratamentos longos (duração > 30 dias)
    val tratamentosLongos = dfRenomeado
      .filter(col("duracao") > 30)
      .select("atendimento", "tratamento", "duracao", "medico")

    // Extrair dados necessários para o gráfico
    // Usando collect e map para extrair os dados sem utilizar encoders explícitos
    val ids: Seq[String] = tratamentosLongos.select("atendimento").collect().map(row => row.get(0).toString)

    // Acesso da coluna "duracao" com a conversão adequada para Double
    val duracoes: Seq[Double] = tratamentosLongos.select("duracao").collect().map(row =>
      row.get(0) match {
        case v: Int => v.toDouble   // Se for inteiro, converte para Double
        case v: Double => v        // Se já for Double, apenas retorna
        case _ => 0.0              // Caso contrário, assume 0.0 (ou outro valor default)
      }
    )

    // Cria o gráfico de linha
    val grafico = Scatter()
      .withX(ids)                       // IDs dos atendimentos no eixo X (Sequência)
      .withY(duracoes)                  // Duração dos tratamentos no eixo Y (Sequência)
      .withName("Duração dos Tratamentos")

    // Layout ajustado
    val layout = Layout()
      .withTitle("Duração de Tratamentos Longos (> 30 Dias)")
      .withXaxis(
        Axis()
          .withTitle("ID do Atendimento")
          .withTickangle(45) // Inclinação dos rótulos do eixo X para melhor leitura
      )
      .withYaxis(
        Axis()
          .withTitle("Duração (dias)")
          .withGridcolor(Color.RGBA(200, 200, 200, 0.5)) // Linhas de grade no eixo Y
      )
      .withPlot_bgcolor(Color.RGBA(245, 245, 245, 1.0)) // Cor de fundo cinza claro
      .withMargin(
        Margin(l = 40, r = 30, t = 50, b = 100) // Margens ajustadas para labels inclinados
      )

    // Gera o gráfico em um arquivo HTML
    val caminhoArquivo = "tratamentos_longos_linha.html"
    Plotly.plot(
      path = caminhoArquivo,
      traces = Seq(grafico),
      layout = layout
    )

    println(s"Gráfico gerado com sucesso: $caminhoArquivo")

    // Exibe os tratamentos longos no console
    tratamentosLongos.show(truncate = false)
  }
}