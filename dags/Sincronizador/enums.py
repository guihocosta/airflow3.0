from enum import IntEnum

class StatusAlocacaoBolsista(IntEnum):
    EM_EDICAO = 0
    DOCUMENTACAO_PENDENTE = 1
    AGUARDANDO_ACEITES = 2
    PENDENTE_DE_AVALIACAO = 3
    EM_AVALIACAO = 4
    ATIVA = 5
    SUSPENSA = 6
    CANCELADA = 7
    FINALIZADA = 8
    INDEFINIDO = 9
    REPROVADA = 10

class TipoDocumentoEnum(IntEnum):
    CARTEIRA_IDENTIDADE = 1
    CARTEIRA_TRABALHO_PREVIDENCIA_SOCIAL = 5
    CARTEIRA_HABILITACAO = 6

class EnumEstadoCivil(IntEnum):
    SOLTEIRO = 1
    CASADO = 2
    SEPARADO = 3
    VIUVO = 4
    DIVORCIADO = 5
    OUTROS = 6

class EnumSexo(IntEnum):
    MASCULINO = 1
    FEMININO = 2

class EnumRegimeCasamento(IntEnum):
    NENHUM =  0
    COMUNHAO_PARCIAL = 1
    COMUNHAO_TOTAL = 2
    SEPARACAO_DE_BENS = 3

class EnumStatusProjeto(IntEnum):
    EM_ANDAMENTO = 0
    CANCELADO = 1
    SUBSTITUIDO = 2
    INDEFINIDO = 3
    FINALIZADO = 4