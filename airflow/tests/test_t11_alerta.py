"""T11 — a falha avisa alguém (spec-dag-carga.md, R8 e seção 9.4).

O R8 diz "nada de intervenção de madrugada", e o que o torna verificável é este
teste: uma carga que falha às 2h da manhã não pode depender de alguém abrir a
interface do Airflow no dia seguinte para descobrir.

Todos os casos são de FORMA — leem o objeto DAG e o fonte, sem rede, sem SMTP e
sem banco. Enviar e-mail de verdade num teste seria trocar um teste por um
incômodo: o canal é o da instalação (a conexão `email_notificacao`), e quem
prova que ele funciona é o primeiro disparo à mão do cutover (seção 9.5).

O alerta segue o precedente da própria instalação, a DAG do comparador
Protheus × Receita: `send_smtp_notification` em `on_failure_callback`.
"""

import re

import pytest

CONEXAO_SMTP = "email_notificacao"
VARIABLE_DESTINATARIO = "cnpj_carga_email_avisos"

# Um e-mail escrito no fonte da DAG. Endereço em código é endereço que continua
# avisando quem já saiu da equipe, e que ninguém troca sem abrir um PR.
EMAIL_LITERAL = re.compile(r"[\w.+-]+@[\w-]+\.[\w.]+")


# --------------------------------------------------------------------------
# O alerta existe, e vale para TODAS as tasks
# --------------------------------------------------------------------------

def test_t11_toda_task_avisa_ao_falhar(tarefas):
    """`on_failure_callback` em `default_args`, não task a task.

    Posto em cada task à mão, ele vira uma lista que alguém esquece de repetir
    na próxima task — e a task esquecida é justamente a que falha calada."""
    sem_alerta = [tid for tid, t in tarefas.items() if not t.on_failure_callback]
    assert not sem_alerta, f"tasks que falham sem avisar ninguém: {sem_alerta}"


def test_t11_a_carga_avisa_ao_falhar(tarefas):
    """A task que mais importa: `carregar` tem `retries=0` (T9), então a
    primeira falha dela é a definitiva — não há segunda chance silenciosa."""
    assert tarefas["carregar"].on_failure_callback


# --------------------------------------------------------------------------
# O canal é o que a instalação já tem
# --------------------------------------------------------------------------

def test_t11_usa_a_conexao_smtp_da_instalacao(fonte_dag):
    assert "send_smtp_notification" in fonte_dag
    assert CONEXAO_SMTP in fonte_dag, (
        f"o alerta tem de sair pela conexão `{CONEXAO_SMTP}`, que já existe no "
        "Airflow do servidor — não por um canal novo inventado aqui"
    )


def test_t11_o_remetente_e_dito_explicitamente(fonte_dag):
    """Medido em 10/09/2026 na mesma instalação: o `extra` da connection está
    VAZIO e o SmtpHook não tem fallback para o remetente. Sem `from_email`,
    todo envio morre em "You should provide `from_email`" — e o alerta que só
    falha na hora de alertar é pior que não ter alerta."""
    assert "from_email" in fonte_dag
    assert "{{ conn." + CONEXAO_SMTP + ".login }}" in fonte_dag, (
        "o remetente sai da própria conexão, resolvido no runtime: se a caixa "
        "mudar, muda num lugar só"
    )


def test_t11_o_destinatario_vem_de_uma_variable(fonte_dag):
    assert "{{ var.value." + VARIABLE_DESTINATARIO + " }}" in fonte_dag


def test_t11_nenhum_email_escrito_no_fonte(fonte_dag):
    achados = EMAIL_LITERAL.findall(fonte_dag)
    assert not achados, f"endereço de e-mail em código: {achados}"


# --------------------------------------------------------------------------
# O aviso diz o suficiente para agir
# --------------------------------------------------------------------------

@pytest.mark.parametrize("pedaco", ["ti.task_id", "ti.dag_id", "ti.log_url"])
def test_t11_o_aviso_diz_onde_olhar(fonte_dag, pedaco):
    """Um e-mail que só diz "a DAG falhou" obriga a abrir a interface para
    descobrir o quê — que é exatamente o trabalho de madrugada que o R8 quer
    evitar."""
    assert pedaco in fonte_dag


def test_t11_sem_canal_duplicado(fonte_dag):
    """`email_on_failure` é o canal antigo do Airflow, por `[smtp]` do
    airflow.cfg. Ligado junto com o callback, manda dois e-mails por falha — e
    o segundo não tem `log_url`."""
    assert "email_on_failure" not in fonte_dag


# --------------------------------------------------------------------------
# O que NÃO pode alertar
# --------------------------------------------------------------------------

def test_t11_degradado_nao_alerta(modulo):
    """`degradado` é sucesso (R5). Um alerta por mês sujo da Receita treina a
    equipe a ignorar o alerta — e aí o alerta que importa também é ignorado."""
    assert modulo.avaliar_desfecho("degradado", "2026-09") == "degradado"


def test_t11_mes_ja_carregado_nao_alerta(tarefas):
    """Sem mês novo a run pula limpa (T7): o short-circuit marca as tasks a
    jusante como `skipped`, e `skipped` não dispara `on_failure_callback`."""
    from airflow.providers.standard.operators.python import ShortCircuitOperator

    assert isinstance(tarefas["detectar_mes"], ShortCircuitOperator)
