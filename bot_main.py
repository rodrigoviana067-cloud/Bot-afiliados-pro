# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════════════════════════════════╗
║          BOT AFILIADOS PRO  –  v9.1  🚀                        ║
║  ✅ Plano único com acesso completo a todos os recursos         ║
║  ✅ Onboarding guiado passo a passo                             ║
║  ✅ Preview antes de postar                                     ║
║  ✅ Histórico de postagens com repost 1 clique                 ║
║  ✅ Templates de copy personalizados                            ║
║  ✅ Blacklist de produtos/lojas                                 ║
║  ✅ Filtro por categoria no auto-poster                         ║
║  ✅ Relatório semanal automático                                ║
║  ✅ Retry automático com backoff                                ║
║  ✅ Fila de postagem assíncrona                                 ║
║  ✅ Comando /status rápido                                      ║
║  ✅ Notificação de vencimento 3 dias antes                      ║
║  ✅ Confirmação antes de deletar                                ║
║  ✅ Botão repostar no histórico                                 ║
║  ✅ Link afiliado por usuário (comissão garantida)              ║
║  ✅ Sistema de pagamento robusto via bot admin                  ║
║  ✅ WhatsApp multi-usuário com bridge pessoal                   ║
║  ✅ Suporte a {preco_original_riscado} em templates             ║
╚══════════════════════════════════════════════════════════════════╝
"""

import os, sys, logging, requests, random, re, hashlib
import json, asyncio, threading, time, hmac
try:
    import asyncpg
except ImportError:
    print("""
╔══════════════════════════════════════════════════════════╗
║  ERRO: asyncpg não instalado!                           ║
║                                                          ║
║  Instale com:                                            ║
║    pip install asyncpg                                   ║
╚══════════════════════════════════════════════════════════╝
""")
    sys.exit(1)
import mercadopago
from datetime import datetime, timedelta
from dataclasses import dataclass, field, replace as dc_replace
from typing import List, Dict, Optional, Tuple, Any
from abc import ABC, abstractmethod
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse, quote
from collections import deque

from bs4 import BeautifulSoup
import aiohttp
from flask import Flask, request as flask_request, jsonify
from dotenv import load_dotenv
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand,
    KeyboardButton, KeyboardButtonRequestChat, ReplyKeyboardMarkup,
    ReplyKeyboardRemove, ChatAdministratorRights,
)
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    filters, ContextTypes, CallbackQueryHandler,
)

load_dotenv()
logger_api = logging.getLogger("ShopeeGraphQL")

# ══════════════════════════════════════════════════════════════
#  CONSTANTES
# ══════════════════════════════════════════════════════════════
# Shopee retorna preços em centavos×1000 (ex: 4990000 = R$49,90)
SHOPEE_CENTAVOS_DIVISOR: int = 100_000
SHOPEE_CENTAVOS_NORMAL:  int = 100
# Tamanho do hash de URL usado como chave de cache/DB
URL_HASH_LENGTH: int = 14
# Desconto máximo considerado válido (acima = dados suspeitos)
MAX_DESCONTO_VALIDO: int = 95
# Desconto mínimo para considerar um produto com desconto real
MIN_DESCONTO_REAL: int = 5


# ══════════════════════════════════════════════════════════════
#  DATACLASSES BASE
# ══════════════════════════════════════════════════════════════
@dataclass
class LinkInfo:
    url_original: str
    url_limpa:    str = ""
    url_hash:     str = ""
    plataforma:   str = ""
    eh_afiliado:  bool = False
    produto_id:   Optional[str] = None

    def __post_init__(self):
        if not self.url_hash:
            self.url_hash = hashlib.md5(self.url_limpa.encode()).hexdigest()[:URL_HASH_LENGTH]


@dataclass
class Produto:
    titulo:         str
    descricao:      str
    preco:          str
    preco_original: str
    imagem:         str
    link:           LinkInfo
    avaliacao:      str = ""
    num_avaliacoes: str = ""
    vendidos:       str = ""
    loja:           str = ""
    categoria:      str = ""
    desconto_pct:   int = 0
    metodo:         str = "desconhecido"
    link_afiliado:  str = ""
    video:          str = ""  # URL do vídeo do produto (mp4)

    def preco_float(self) -> float:
        try:
            return float(self.preco.replace(",", "."))
        except Exception:
            return 0.0


# ══════════════════════════════════════════════════════════════
#  API GRAPHQL SHOPEE
# ══════════════════════════════════════════════════════════════
class ShopeeAffiliateGraphQL:
    def __init__(self, app_id: str, api_key: str):
        self.app_id   = app_id
        self.api_key  = api_key
        self.endpoint = "https://open-api.affiliate.shopee.com.br/graphql"
        self._session: Optional[aiohttp.ClientSession] = None

    def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30)
            )
        return self._session

    async def fechar(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    def _generate_signature(self, timestamp: str, payload: str) -> str:
        raw = f"{self.app_id}{timestamp}{payload}{self.api_key}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    async def _make_request(self, query: str, variables: Dict = None) -> Dict:
        timestamp = str(int(time.time()))
        payload   = json.dumps({"query": query, "variables": variables or {}},
                               separators=(",", ":"), ensure_ascii=False)
        signature   = self._generate_signature(timestamp, payload)
        auth_header = f"SHA256 Credential={self.app_id}, Timestamp={timestamp}, Signature={signature}"
        headers     = {"Content-Type": "application/json", "Authorization": auth_header}
        session = self._get_session()
        async with session.post(self.endpoint, headers=headers, data=payload) as response:
            resp_text = await response.text()
            if response.status != 200:
                raise Exception(f"HTTP {response.status}: {resp_text[:200]}")
            return json.loads(resp_text)

    async def get_product_by_link(self, product_url: str) -> Optional[Produto]:
        # Resolve link curto antes de tentar extrair IDs
        url_resolvida = product_url
        if any(d in product_url for d in ["s.shopee","shopee.ee","shp.ee","br.shp.ee"]):
            url_resolvida = await self._resolver_url(product_url)
        item_id, shop_id = self._extract_ids(url_resolvida)
        # Tenta também na URL original se não encontrou
        if not item_id:
            item_id, shop_id = self._extract_ids(product_url)
        if not item_id:
            return None
        product_url = url_resolvida
        q = f'''{{
  productOfferV2(itemId: {int(item_id)}, shopId: {int(shop_id) if shop_id else 0}, limit: 1) {{
    nodes {{
      itemId shopId productName priceMin priceMax priceDiscountRate
      imageUrl productLink offerLink commissionRate sales ratingStar shopName
    }}
  }}
}}'''
        try:
            result = await self._make_request(q, {})
            if result.get("errors"):
                return None
            nodes = result.get("data", {}).get("productOfferV2", {}).get("nodes", [])
            if not nodes:
                return None
            return self._create_product_from_node(nodes[0], product_url)
        except Exception as e:
            logger_api.error(f"productOfferV2 falhou: {e}")
            return None

    async def buscar_melhores_promocoes(self, limite: int = 50,
                                        categoria: str = "") -> List[Produto]:
        """
        Busca produtos. A API NAO filtra por categoria.
        Buscamos volume grande e filtramos localmente por palavras-chave.
        """
        # API Shopee limita a 50 por query — 4 sortTypes = até ~150 únicos
        LIM_API = 50  # máximo permitido pela API
        sort_types = [2, 1, 4, 3]  # 2=maior desconto, 1=relevância, 4=novidades, 3=vendidos
        todos = []
        vistos: set = set()
        for st in sort_types:
            q = (f'{{ productOfferV2(limit: {LIM_API}, sortType: {st}) {{' 
                 f'  nodes {{ itemId shopId productName priceMin priceMax priceDiscountRate'
                 f'  imageUrl offerLink commissionRate sales ratingStar shopName }} }} }}')
            try:
                result = await self._make_request(q, {})
                if result.get("errors"):
                    logger_api.warning(f"GraphQL sortType={st}: {result['errors'][0]['message']}")
                    continue
                nodes = result.get("data", {}).get("productOfferV2", {}).get("nodes", [])
                logger_api.info(f"buscar sortType={st}: {len(nodes)} nodes")
                for node in nodes:
                    item_id = str(node.get("itemId", ""))
                    if item_id and item_id not in vistos:
                        vistos.add(item_id)
                        try:
                            p = self._create_product_from_node(node, node.get("offerLink", ""))
                            if p and p.desconto_pct >= MIN_DESCONTO_REAL and p.preco and p.imagem:
                                todos.append(p)
                        except Exception:
                            pass
            except Exception as e:
                logger_api.error(f"buscar sortType={st} falhou: {e}")

        todos_ord = sorted(todos, key=lambda x: x.desconto_pct, reverse=True)
        logger_api.info(f"buscar_melhores_promocoes: {len(todos_ord)} produtos unicos")

        if categoria and categoria != "todos":
            filtrados = [p for p in todos_ord if _produto_bate_nicho(p, categoria)]
            logger_api.info(f"nicho={categoria}: {len(filtrados)}/{len(todos_ord)} compativeis")
            return filtrados
        return todos_ord

    async def buscar_por_nicho_direto(self, nicho: str, limite: int = 30) -> List[Produto]:
        """
        Busca produtos de um nicho específico usando palavras-chave diretas.
        Usado como complemento quando o pool geral tem poucos produtos do nicho.
        """
        # Termos de busca por nicho
        termos = {
            "eletronicos":     ["celular samsung","notebook","fone bluetooth","smartwatch"],
            "moda":            ["tênis","cinto","óculos de sol","mochila"],
            "moda_feminina":   ["vestido feminino","blusa feminina","sandália feminina","bolsa feminina"],
            "moda_masculina":  ["camiseta masculina","camisa masculina","tênis masculino","bermuda masculina"],
            "casa":            ["panela","colchão","tapete","organizador"],
            "beleza":          ["shampoo","condicionador","desodorante","protetor solar"],
            "beleza_feminina": ["maquiagem","batom","sérum facial","perfume feminino"],
            "beleza_masculina":["barbeador","gel de barbear","perfume masculino","pomada capilar"],
            "esportes":        ["tênis esportivo","bicicleta","academia","suplemento"],
            "games":           ["headset gamer","controle ps5","mouse gamer","cadeira gamer"],
            "automotivo":      ["suporte celular carro","câmera ré","som automotivo","pneu"],
            "bebes":           ["fralda","carrinho bebê","brinquedo infantil","mamadeira"],
            "alimentos":       ["whey protein","café","chocolate","pasta amendoim"],
        }
        palavras = termos.get(nicho, [])
        if not palavras:
            return []

        todos = []
        vistos: set = set()
        termo = random.choice(palavras)  # busca um termo aleatório por ciclo

        q = (f'{{ productOfferV2(limit: {limite}, sortType: 2, keyword: "{termo}") {{'
             f'  nodes {{ itemId shopId productName priceMin priceMax priceDiscountRate'
             f'  imageUrl offerLink commissionRate sales ratingStar shopName }} }} }}')
        try:
            result = await self._make_request(q, {})
            if not result.get("errors"):
                nodes = result.get("data", {}).get("productOfferV2", {}).get("nodes", [])
                for node in nodes:
                    item_id = str(node.get("itemId", ""))
                    if item_id and item_id not in vistos:
                        vistos.add(item_id)
                        try:
                            p = self._create_product_from_node(node, node.get("offerLink",""))
                            if p and p.desconto_pct >= MIN_DESCONTO_REAL and p.preco and p.imagem:
                                todos.append(p)
                        except Exception:
                            pass
        except Exception as e:
            logger_api.warning(f"buscar_por_nicho_direto {nicho}: {e}")
        return todos

    @staticmethod
    def _montar_url_imagem_static(imageUrl: str) -> str:
        if not imageUrl: return ""
        imageUrl = str(imageUrl).strip()
        if imageUrl.startswith("http"): return imageUrl
        # Hash da imagem Shopee — tenta CDNs diferentes
        # Formato: xxxxxxxxxxxxxx (hash sem extensão)
        cdn = "https://down-br.img.susercontent.com/file/"
        return f"{cdn}{imageUrl}"

    def _create_product_from_node(self, node: Dict, original_url: str) -> Produto:
        def to_float(v, d=0.0):
            if v is None: return d
            try: return float(str(v).replace(",", "."))
            except Exception: return d

        def fmt(v):
            try: return f"{float(v):.2f}" if v and float(v) > 0 else ""
            except Exception: return str(v) if v else ""

        preco_raw = to_float(node.get("priceMin"))
        orig_raw  = to_float(node.get("priceMax"))

        # API Shopee afiliados retorna preços em centavos (100000 = R$1,00)
        # Ex: 4990000 = R$49,90 | 129900000 = R$1.299,00
        def normalizar_preco(v):
            if v <= 0: return 0.0
            # Se maior que 10000, divide por 100000 (centavos Shopee)
            if v >= 10000: return round(v / SHOPEE_CENTAVOS_DIVISOR, 2)
            # Entre 1000 e 9999, pode ser centavos normais (divide por 100)
            if v >= 1000: return round(v / SHOPEE_CENTAVOS_NORMAL, 2)
            return round(v, 2)  # já em reais

        preco = normalizar_preco(preco_raw)
        orig  = normalizar_preco(orig_raw)

        if orig > preco > 0:
            desconto = min(round((1 - preco / orig) * 100), 99)
        else:
            orig     = 0.0
            desconto = 0

        if desconto > MAX_DESCONTO_VALIDO:
            orig     = 0.0
            desconto = 0

        rating_star = to_float(node.get("ratingStar"))
        item_id = node.get("itemId", "")
        shop_id = node.get("shopId", "")
        if item_id and shop_id:
            url_produto = f"https://shopee.com.br/product/{shop_id}/{item_id}"
        elif original_url and original_url.startswith("http"):
            url_produto = original_url
        else:
            url_produto = f"https://shopee.com.br/product/{item_id}"

        offer_link = node.get("offerLink") or node.get("productLink") or ""

        link_info = LinkInfo(
            url_original=url_produto,
            url_limpa=url_produto,
            url_hash=hashlib.md5(url_produto.encode()).hexdigest()[:URL_HASH_LENGTH],
            plataforma="shopee",
        )
        # Processar videoUrl — pode vir como string ou lista
        raw_video = node.get("videoUrl", "") or ""
        if isinstance(raw_video, list):
            raw_video = raw_video[0] if raw_video else ""
        video_url = str(raw_video).strip() if raw_video else ""
        return Produto(
            titulo=(node.get("productName") or "")[:120],
            descricao="",
            preco=fmt(preco),
            preco_original=fmt(orig) if orig > preco > 0 else "",
            imagem=self._montar_url_imagem_static(node.get("imageUrl", "")),
            link=link_info,
            avaliacao=f"{float(rating_star):.1f}" if rating_star > 0 else "",
            num_avaliacoes="",
            vendidos=str(node.get("sales", "")) if node.get("sales") else "",
            loja=node.get("shopName", ""),
            desconto_pct=desconto,
            metodo="shopee_graphql",
            link_afiliado=offer_link,
            video=video_url,
        )

    async def _resolver_url(self, url: str) -> str:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(url, allow_redirects=True,
                                 timeout=aiohttp.ClientTimeout(total=8)) as resp:
                    return str(resp.url)
        except Exception:
            return url

    def _extract_ids(self, url: str) -> tuple:
        patterns = [
            (r"/product/(\d+)/(\d+)",          lambda m: (m.group(2), m.group(1))),
            (r"-i\.(\d+)\.(\d+)",              lambda m: (m.group(2), m.group(1))),
            (r"/[^/?]+/(\d{7,10})/(\d{10,})",  lambda m: (m.group(2), m.group(1))),
            (r"itemid=(\d+).*shopid=(\d+)",    lambda m: (m.group(1), m.group(2))),
            (r"shopid=(\d+).*itemid=(\d+)",    lambda m: (m.group(2), m.group(1))),
        ]
        for pattern, extractor in patterns:
            match = re.search(pattern, url, re.IGNORECASE)
            if match:
                return extractor(match)
        return None, None



# ══════════════════════════════════════════════════════════════
#  API MERCADO LIVRE — busca produtos com desconto
# ══════════════════════════════════════════════════════════════
class MercadoLivreAPI:
    BASE = "https://api.mercadolibre.com"

    @classmethod
    async def _get_token(cls) -> str:
        """Obtém token via OAuth com refresh automático."""
        return await ml_auth.get_token()

    @classmethod
    async def buscar_promocoes(cls, limite: int = 50) -> List["Produto"]:
        """Busca produtos em promoção usando token OAuth."""
        token = await cls._get_token()
        if not token:
            logger.warning("[ML] Sem token OAuth")
            return []
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        # Busca produtos com desconto
        url = f"{cls.BASE}/sites/MLB/search?sort=price_asc&condition=new&limit={limite}"
        
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(url, headers=headers, timeout=15) as r:
                    if r.status == 200:
                        data = await r.json()
                        items = data.get("results", [])
                        
                        produtos = []
                        for item in items:
                            if item.get("original_price") and item.get("original_price") > item.get("price", 0):
                                # Tem desconto
                                p = cls._parse_item(item)
                                if p:
                                    produtos.append(p)
                        
                        logger.info(f"[ML] {len(produtos)} produtos com desconto")
                        return produtos
                    elif r.status == 401:
                        # Token expirou, força renovação
                        await ml_auth.refresh_access_token()
                        return await cls.buscar_promocoes(limite)
                    else:
                        logger.warning(f"[ML] Busca falhou: {r.status}")
                        return []
        except Exception as e:
            logger.error(f"[ML] buscar_promocoes: {e}")
            return []
    
    @staticmethod
    def _parse_item(item: dict) -> Optional["Produto"]:
        """Converte item em Produto."""
        try:
            preco = float(item.get("price", 0))
            original = float(item.get("original_price", 0))
            
            if preco <= 0 or original <= preco:
                return None
            
            desconto = round((1 - preco / original) * 100)
            
            # Cria link temporário
            link_info = LinkInfo(
                url_original=item.get("permalink", ""),
                url_limpa="",
                plataforma="mercadolivre",
            )
            
            imagem = item.get("thumbnail", "")
            if imagem:
                imagem = imagem.replace("-I.jpg", "-O.jpg")
            
            return Produto(
                titulo=item.get("title", "")[:120],
                descricao="",
                preco=f"{preco:.2f}",
                preco_original=f"{original:.2f}",
                imagem=imagem,
                link=link_info,
                loja=item.get("seller", {}).get("nickname", ""),
                desconto_pct=desconto,
                metodo="mercadolivre_api",
                link_afiliado=item.get("permalink", ""),
            )
        except Exception as e:
            logger.warning(f"[ML] parse_item: {e}")
            return None
# ══════════════════════════════════════════════════════════════
#  CONFIGURAÇÕES
# ══════════════════════════════════════════════════════════════

# Planos disponíveis com limites
_PLANO_BASE: Dict = {
    "nome": "⭐ Mensal",
    "dias": 30,
    "canais_tg": 999,
    "grupos_wa": 999,
    "templates_custom": 999,
    "auto_poster": True,
    "agendamento": True,
    "relatorio": True,
    "preco": 19.99,
}
_PLANO_REFERRAL: Dict = {**_PLANO_BASE, "nome": "🎁 Bônus"}
_PLANO_TESTE: Dict = {**_PLANO_BASE, "nome": "🎁 Teste Grátis", "dias": 7}

PLANOS: Dict[str, Dict] = {
    # Plano único — acesso completo por R$19,99/mês
    "mensal":   _PLANO_BASE,
    # Período de teste gratuito
    "teste":    _PLANO_TESTE,
    # Aliases legados — mantidos para compatibilidade com assinantes antigos
    "basic":    _PLANO_BASE,
    "pro":      _PLANO_BASE,
    "premium":  _PLANO_BASE,
    "referral": _PLANO_REFERRAL,
}

CATEGORIAS_AUTO = {
    "todos":           "🌐 Todos",
    "eletronicos":     "📱 Eletrônicos",
    "moda":            "👗 Moda (Geral)",
    "moda_feminina":   "👩 Moda Feminina",
    "moda_masculina":  "👨 Moda Masculina",
    "casa":            "🏠 Casa e Decoração",
    "beleza":          "💄 Beleza (Geral)",
    "beleza_feminina": "💅 Beleza Feminina",
    "beleza_masculina":"🧔 Beleza Masculina",
    "esportes":        "⚽ Esportes",
    "games":           "🎮 Games",
    "automotivo":      "🚗 Automotivo",
    "bebes":           "👶 Bebês",
    "alimentos":       "🍎 Alimentos",
}


@dataclass(frozen=True)
class Config:
    TOKEN:                 str   = os.getenv("TELEGRAM_BOT_TOKEN", "")
    ID_ADMIN:              int   = int(os.getenv("ADMIN_ID", "0"))
    MP_ACCESS_TOKEN:       str   = os.getenv("MP_ACCESS_TOKEN", "")
    MP_WEBHOOK_SECRET:     str   = os.getenv("MP_WEBHOOK_SECRET", "")
    MP_PLAN_ID:            str   = os.getenv("MP_PLAN_ID", "cf0575a4464d41258eebbe57c9484474")
    LINK_PAGAMENTO:        str   = os.getenv("LINK_PAGAMENTO",
                                   "https://www.mercadopago.com.br/subscriptions/checkout"
                                   "?preapproval_plan_id=cf0575a4464d41258eebbe57c9484474")
    DATABASE_URL:          str   = os.getenv("DATABASE_URL", "")
    SHOPEE_APP_ID:         str   = os.getenv("SHOPEE_APP_ID", "")
    SHOPEE_SECRET:         str   = os.getenv("SHOPEE_SECRET", "")
    SUPORTE_LINK:          str   = os.getenv("SUPORTE_LINK", "https://t.me/rodrigoviana20")
    WA_BRIDGE_URL:         str   = os.getenv("WA_BRIDGE_URL", "http://localhost:3000")
    ML_ACCESS_TOKEN:       str   = os.getenv("ML_ACCESS_TOKEN", "")
    ML_CLIENT_ID:          str   = os.getenv("ML_CLIENT_ID", "")
    ML_SECRET_KEY:         str   = os.getenv("ML_SECRET_KEY", "")
    ML_USER_ID:            str   = os.getenv("ML_USER_ID", "")
    REQUEST_TIMEOUT:       int   = 15
    CACHE_TTL:             int   = 3600
    BONUS_CONVIDADO:       int   = 7
    BONUS_CONVIDANTE:      int   = 30
    AUTO_POSTER_INTERVALO: int   = int(os.getenv("AUTO_POSTER_INTERVALO", "30"))
    MAX_RETRY:             int   = 3
    RETRY_DELAY:           int   = 5

    HORARIOS_POSTAGEM: List[str] = field(default_factory=lambda: [
        "08:00","10:00","12:00","14:00","16:00","18:00","20:00","22:00",
    ])
    PLATAFORMAS: Dict = field(default_factory=lambda: {
        "shopee":       {"dominios": ["shopee.com.br","s.shopee.com.br","shopee.ee","shp.ee","br.shp.ee","shopee.com","m.shopee.com.br"], "emoji": "🛍️","cor": "🟠"},
        "amazon":       {"dominios": ["amazon.com.br","amzn.to","amazon.com"],              "emoji": "📦","cor": "🟡"},
        "magalu": {"dominios": ["magazineluiza.com.br", "magalu.com.br", "onelink.me", "divulgador.magalu.com"], "emoji": "🛒", "cor": "🔵"},
        "aliexpress":   {"dominios": ["aliexpress.com","s.click.aliexpress.com","ali.ski"], "emoji": "🇨🇳","cor": "🔴"},
        "mercadolivre": {"dominios": ["mercadolivre.com.br","ml.com.br","meli.bz","meli.la","mercadolibre.com","produto.mercadolivre.com.br","lista.mercadolivre.com.br"], "emoji": "🟡","cor": "🟡"},
        "hotmart":      {"dominios": ["hotmart.com","go.hotmart.com"],                       "emoji": "🔥","cor": "🟠"},
        "kiwify":       {"dominios": ["kiwify.com.br","pay.kiwify.com.br"],                 "emoji": "💎","cor": "🟣"},
        "monetizze":    {"dominios": ["monetizze.com.br","app.monetizze.com.br"],           "emoji": "💰","cor": "🟢"},
    })

    def __post_init__(self):
        if not self.TOKEN or ":" not in self.TOKEN:
            raise ValueError("❌ TELEGRAM_BOT_TOKEN inválido ou não configurado no .env!")

try:
    cfg = Config()
except ValueError as e:
    print(e); sys.exit(1)

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    level=logging.INFO,
    handlers=[logging.FileHandler("bot.log", encoding="utf-8"), logging.StreamHandler()],
)
logger = logging.getLogger("BotAfiliados")

telegram_app: Optional[Application] = None
_main_loop:   Optional[asyncio.AbstractEventLoop] = None


# ══════════════════════════════════════════════════════════════
#  CACHE
# ══════════════════════════════════════════════════════════════
class Cache:
    def __init__(self):
        self._data: Dict[str, Tuple[Any, datetime]] = {}
        self._lock = threading.Lock()

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            entry = self._data.get(key)
            if entry:
                val, exp = entry
                if datetime.now() < exp:
                    return val
                del self._data[key]
            return None

    def set(self, key: str, val: Any, ttl: int = None):
        with self._lock:
            self._data[key] = (val, datetime.now() + timedelta(seconds=ttl or cfg.CACHE_TTL))

    def delete(self, key: str):
        with self._lock:
            self._data.pop(key, None)

cache = Cache()


# ══════════════════════════════════════════════════════════════
#  BANCO DE DADOS — NeonDB via asyncpg
# ══════════════════════════════════════════════════════════════
# Colunas permitidas para aff_{plataforma} — evita SQL injection
_AFF_COLS_PERMITIDAS = {
    "shopee", "amazon", "mercadolivre", "hotmart",
    "kiwify", "monetizze", "magalu", "aliexpress",
}

def _to_pg(sql: str) -> str:
    """Converte placeholders %s → $1, $2, $3... para asyncpg."""
    n = 0
    out = []
    i = 0
    while i < len(sql):
        if sql[i] == '%' and i + 1 < len(sql) and sql[i + 1] == 's':
            n += 1
            out.append(f'${n}')
            i += 2
        else:
            out.append(sql[i])
            i += 1
    return ''.join(out)


class Database:
    """
    Banco NeonDB via asyncpg.
    Mantém API 100% síncrona externamente — internamente usa um loop
    dedicado em thread separada, compatível com Flask, workers e
    handlers do Telegram sem risco de deadlock.
    """

    def __init__(self):
        self._pool: asyncpg.Pool = None
        self._db_loop: asyncio.AbstractEventLoop = None
        self._ready = threading.Event()
        self._start_db_thread()
        if not self._ready.wait(timeout=30):
            raise RuntimeError("❌ Timeout ao conectar no NeonDB (30s). Verifique DATABASE_URL.")
        self._setup()
        self._migrar()

    # ── Loop dedicado ─────────────────────────────────────────────
    def _start_db_thread(self):
        def _run():
            self._db_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._db_loop)
            async def _init():
                dsn = os.getenv("DATABASE_URL")
                if not dsn:
                    raise ValueError("DATABASE_URL não definida no .env")
                self._pool = await asyncpg.create_pool(
                    dsn=dsn, min_size=2, max_size=20,
                    command_timeout=30,
                )
                self._ready.set()
            try:
                self._db_loop.run_until_complete(_init())
                self._db_loop.run_forever()
            except Exception as e:
                logger.error(f"[DB] Falha ao iniciar pool asyncpg: {e}")
                self._ready.set()  # libera o wait para o erro ser tratado

        t = threading.Thread(target=_run, daemon=True, name="DBLoop")
        t.start()

    def _run(self, coro):
        """Executa coroutine no loop do DB de qualquer contexto (sync ou async)."""
        if self._db_loop is None or not self._db_loop.is_running():
            raise RuntimeError("[DB] Loop do banco não está rodando.")
        future = asyncio.run_coroutine_threadsafe(coro, self._db_loop)
        return future.result(timeout=30)

    # ── Executor principal ────────────────────────────────────────
    async def _exec_async(self, sql: str, params=(), fetch: str = None):
        sql_pg = _to_pg(sql)
        async with self._pool.acquire() as conn:
            if fetch == "one":
                row = await conn.fetchrow(sql_pg, *params)
                return dict(row) if row else None
            elif fetch == "all":
                rows = await conn.fetch(sql_pg, *params)
                return [dict(r) for r in rows]
            else:
                return await conn.execute(sql_pg, *params)

    def _exec(self, sql: str, params=(), fetch: str = None):
        return self._run(self._exec_async(sql, params, fetch))

    # ── helpers ───────────────────────────────────────────────────
    def _parse_json_list(self, val) -> List:
        try:    return json.loads(val or "[]")
        except: return []

    @staticmethod
    def _parse_date(val) -> str:
        """
        Normaliza QUALQUER formato de data para YYYY-MM-DD.
        Suporta: YYYY-MM-DD, YYYY-MM, DD/MM/YYYY, datetime, date, ISO, RFC2822.
        Para YYYY-MM (formato antigo do Vercel) → último dia do mês.
        """
        import re as _re
        from calendar import monthrange as _mr
        if not val: return ""
        # datetime/date objects
        if hasattr(val, "strftime"): return val.strftime("%Y-%m-%d")
        s = str(val).strip()
        # YYYY-MM-DD — já correto
        if _re.match(r"^\d{4}-\d{2}-\d{2}$", s): return s
        # YYYY-MM-DD com hora: '2026-04-25T...' ou '2026-04-25 ...'
        m = _re.match(r"^(\d{4}-\d{2}-\d{2})[T ]", s)
        if m: return m.group(1)
        # YYYY-MM (formato Vercel — sem dia) → último dia do mês
        m = _re.match(r"^(\d{4})-(\d{2})$", s)
        if m:
            y, mo = int(m.group(1)), int(m.group(2))
            last = _mr(y, mo)[1]
            return f"{y:04d}-{mo:02d}-{last:02d}"
        # DD/MM/YYYY (formato brasileiro)
        m = _re.match(r"^(\d{2})/(\d{2})/(\d{4})$", s)
        if m:
            return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
        # Formatos RFC/textual
        for fmt in ("%a, %d %b %Y %H:%M:%S %Z", "%a, %d %b %Y %H:%M:%S %z",
                    "%a %d %b %Y %H:%M:%S %Z", "%d %b %Y %H:%M:%S %Z",
                    "%d %b %Y", "%b %d %Y"):
            try:    return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
            except: continue
        # Último recurso: dateutil
        try:
            from dateutil import parser as du
            return du.parse(s).strftime("%Y-%m-%d")
        except: pass
        logger.warning(f"[DB] _parse_date: formato desconhecido '{s[:40]}'")
        return ""

    # ── Setup (CREATE TABLE IF NOT EXISTS) ────────────────────────
    def _setup(self):
        async def _do():
            async with self._pool.acquire() as conn:
                await conn.execute("""
                CREATE TABLE IF NOT EXISTS assinantes (
                    id                  BIGINT PRIMARY KEY,
                    vencimento          TEXT    NOT NULL,
                    estilo              TEXT    DEFAULT 'padrao',
                    limite_canais       INTEGER DEFAULT 10,
                    canais_tg           TEXT    DEFAULT '[]',
                    canais_tg_ativos    TEXT    DEFAULT '[]',
                    grupos_wa           TEXT    DEFAULT '[]',
                    grupos_wa_ativos    TEXT    DEFAULT '[]',
                    grupos_wa_nomes     TEXT    DEFAULT '{}',
                    wa_bridge_url       TEXT    DEFAULT '',
                    modo_auto           INTEGER DEFAULT 0,
                    min_desconto        INTEGER DEFAULT 20,
                    ultimo_auto_post    TEXT    DEFAULT '',
                    ativo               INTEGER DEFAULT 1,
                    ativado_em          TEXT,
                    plano               TEXT    DEFAULT 'mensal',
                    email               TEXT,
                    nome                TEXT,
                    username            TEXT,
                    aff_shopee          TEXT    DEFAULT '',
                    aff_amazon          TEXT    DEFAULT '',
                    aff_mercadolivre    TEXT    DEFAULT '',
                    aff_hotmart         TEXT    DEFAULT '',
                    aff_kiwify          TEXT    DEFAULT '',
                    aff_monetizze       TEXT    DEFAULT '',
                    aff_magalu          TEXT    DEFAULT '',
                    aff_aliexpress      TEXT    DEFAULT '',
                    onboarding_step     INTEGER DEFAULT 0,
                    blacklist_lojas     TEXT    DEFAULT '[]',
                    blacklist_produtos  TEXT    DEFAULT '[]',
                    categoria_auto      TEXT    DEFAULT 'todos',
                    templates_custom    TEXT    DEFAULT '[]',
                    ultimo_relatorio    TEXT    DEFAULT '',
                    nichos_tg           TEXT    DEFAULT '{}',
                    nichos_wa           TEXT    DEFAULT '{}',
                    template_ativo_idx  INTEGER DEFAULT -1,
                    templates_tg        TEXT    DEFAULT '{}',
                    templates_wa        TEXT    DEFAULT '{}'
                )""")
                await conn.execute("""
                CREATE TABLE IF NOT EXISTS links (
                    id         SERIAL PRIMARY KEY,
                    user_id    BIGINT NOT NULL, url TEXT NOT NULL,
                    titulo     TEXT,   plataforma TEXT,
                    preco      TEXT,   imagem TEXT,
                    video      TEXT    DEFAULT '',
                    url_hash   TEXT    UNIQUE,
                    criado_em  TEXT
                )""")
                await conn.execute("""
                CREATE TABLE IF NOT EXISTS agendamentos (
                    id         SERIAL PRIMARY KEY,
                    user_id    BIGINT NOT NULL, link_id INTEGER,
                    url        TEXT NOT NULL,   url_hash TEXT,
                    titulo     TEXT,            canal TEXT,
                    horario    TEXT,
                    destinos   TEXT DEFAULT 'telegram',
                    status     TEXT DEFAULT 'pendente',
                    criado_em  TEXT, postado_em TEXT,
                    UNIQUE(user_id, url_hash, horario)
                )""")
                await conn.execute("""
                CREATE TABLE IF NOT EXISTS historico (
                    id         SERIAL PRIMARY KEY,
                    user_id    BIGINT, hash TEXT,
                    url        TEXT DEFAULT '',   titulo TEXT DEFAULT '',
                    imagem     TEXT DEFAULT '',   canal TEXT,
                    destino    TEXT DEFAULT 'telegram',
                    video      TEXT DEFAULT '',
                    postado_em TEXT, status TEXT,
                    erro TEXT,  metodo TEXT,
                    preco      TEXT DEFAULT '',
                    desconto   INTEGER DEFAULT 0
                )""")
                await conn.execute("""
                CREATE TABLE IF NOT EXISTS auto_postagens (
                    id         SERIAL PRIMARY KEY,
                    user_id    BIGINT NOT NULL DEFAULT 0,
                    item_hash  TEXT,   titulo TEXT,
                    preco TEXT, desconto INTEGER, postado_em TEXT,
                    UNIQUE(user_id, item_hash)
                )""")
                await conn.execute("""
                CREATE TABLE IF NOT EXISTS stats (
                    user_id         BIGINT PRIMARY KEY,
                    total_postagens INTEGER DEFAULT 0,
                    total_wa        INTEGER DEFAULT 0,
                    total_semana    INTEGER DEFAULT 0,
                    total_mes       INTEGER DEFAULT 0,
                    ultima_postagem TEXT,
                    semana_inicio   TEXT DEFAULT ''
                )""")
                await conn.execute("""
                CREATE TABLE IF NOT EXISTS pagamentos (
                    id         SERIAL PRIMARY KEY,
                    user_id    BIGINT, order_id TEXT UNIQUE,
                    status     TEXT,   valor FLOAT,
                    email      TEXT,   metodo TEXT,
                    plano      TEXT DEFAULT 'mensal',
                    criado_em  TEXT,   processado INTEGER DEFAULT 0
                )""")
                await conn.execute("""
                CREATE TABLE IF NOT EXISTS emails (
                    email TEXT PRIMARY KEY, user_id BIGINT, criado_em TEXT
                )""")
                await conn.execute("""
                CREATE TABLE IF NOT EXISTS testes (
                    user_id BIGINT PRIMARY KEY, inicio TEXT, fim TEXT
                )""")
                await conn.execute("""
                CREATE TABLE IF NOT EXISTS referrals (
                    id SERIAL PRIMARY KEY,
                    referrer_id BIGINT, referred_id BIGINT UNIQUE,
                    criado_em TEXT, status TEXT DEFAULT 'pendente'
                )""")
                await conn.execute("""
                CREATE TABLE IF NOT EXISTS notificacoes (
                    id SERIAL PRIMARY KEY, tipo TEXT, user_id BIGINT,
                    mensagem TEXT, criado_em TEXT, lida INTEGER DEFAULT 0
                )""")
                await conn.execute("""
                CREATE TABLE IF NOT EXISTS bot_chats (
                    chat_id TEXT PRIMARY KEY, titulo TEXT, tipo TEXT, criado_em TEXT
                )""")
                await conn.execute("""
                CREATE TABLE IF NOT EXISTS copies_custom (
                    user_id BIGINT NOT NULL, url_hash TEXT NOT NULL,
                    copy TEXT NOT NULL, criado_em TEXT,
                    PRIMARY KEY (user_id, url_hash)
                )""")
                await conn.execute("""
                CREATE TABLE IF NOT EXISTS fila_postagem (
                    id SERIAL PRIMARY KEY, user_id BIGINT,
                    url TEXT, url_hash TEXT, canal TEXT,
                    destino TEXT DEFAULT 'telegram',
                    tentativas INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'pendente',
                    criado_em TEXT, erro TEXT
                )""")
                for idx in [
                    "CREATE INDEX IF NOT EXISTS idx_links_user  ON links(user_id)",
                    "CREATE INDEX IF NOT EXISTS idx_agenda_user ON agendamentos(user_id)",
                    "CREATE INDEX IF NOT EXISTS idx_hist_user   ON historico(user_id)",
                    "CREATE INDEX IF NOT EXISTS idx_hist_data   ON historico(postado_em)",
                    "CREATE INDEX IF NOT EXISTS idx_fila_status ON fila_postagem(status)",
                    "CREATE INDEX IF NOT EXISTS idx_auto_user   ON auto_postagens(user_id)",
                ]:
                    await conn.execute(idx)
        self._run(_do())

    def _migrar(self):
        """Adiciona colunas novas em tabelas existentes (compatibilidade total)."""
        migracoes = [
            ("assinantes", "modo_auto",           "INTEGER DEFAULT 0"),
            ("assinantes", "min_desconto",         "INTEGER DEFAULT 20"),
            ("assinantes", "ultimo_auto_post",     "TEXT DEFAULT ''"),
            ("assinantes", "limite_canais",        "INTEGER DEFAULT 10"),
            ("assinantes", "canais_tg",            "TEXT DEFAULT '[]'"),
            ("assinantes", "canais_tg_ativos",     "TEXT DEFAULT '[]'"),
            ("assinantes", "grupos_wa",            "TEXT DEFAULT '[]'"),
            ("assinantes", "grupos_wa_ativos",     "TEXT DEFAULT '[]'"),
            ("assinantes", "grupos_wa_nomes",      "TEXT DEFAULT '{}'"),
            ("assinantes", "wa_bridge_url",        "TEXT DEFAULT ''"),
            ("assinantes", "plano",                "TEXT DEFAULT 'mensal'"),
            ("assinantes", "email",                "TEXT"),
            ("assinantes", "nome",                 "TEXT"),
            ("assinantes", "username",             "TEXT"),
            ("assinantes", "aff_shopee",           "TEXT DEFAULT ''"),
            ("assinantes", "aff_amazon",           "TEXT DEFAULT ''"),
            ("assinantes", "aff_mercadolivre",     "TEXT DEFAULT ''"),
            ("assinantes", "aff_hotmart",          "TEXT DEFAULT ''"),
            ("assinantes", "aff_kiwify",           "TEXT DEFAULT ''"),
            ("assinantes", "aff_monetizze",        "TEXT DEFAULT ''"),
            ("assinantes", "aff_magalu",           "TEXT DEFAULT ''"),
            ("assinantes", "aff_aliexpress",       "TEXT DEFAULT ''"),
            ("assinantes", "onboarding_step",      "INTEGER DEFAULT 0"),
            ("assinantes", "blacklist_lojas",      "TEXT DEFAULT '[]'"),
            ("assinantes", "blacklist_produtos",   "TEXT DEFAULT '[]'"),
            ("assinantes", "categoria_auto",       "TEXT DEFAULT 'todos'"),
            ("assinantes", "templates_custom",     "TEXT DEFAULT '[]'"),
            ("assinantes", "ultimo_relatorio",     "TEXT DEFAULT ''"),
            ("assinantes", "nichos_tg",            "TEXT DEFAULT '{}'"),
            ("assinantes", "nichos_wa",            "TEXT DEFAULT '{}'"),
            ("assinantes", "template_ativo_idx",   "INTEGER DEFAULT -1"),
            ("assinantes", "templates_tg",         "TEXT DEFAULT '{}'"),
            ("assinantes", "templates_wa",         "TEXT DEFAULT '{}'"),
            ("assinantes", "estilo",               "TEXT DEFAULT 'padrao'"),
            ("assinantes", "ativado_em",           "TEXT"),
            ("links",      "video",                "TEXT DEFAULT ''"),
            ("historico",  "video",                "TEXT DEFAULT ''"),
            ("historico",  "url",                  "TEXT DEFAULT ''"),
            ("historico",  "titulo",               "TEXT DEFAULT ''"),
            ("historico",  "imagem",               "TEXT DEFAULT ''"),
            ("historico",  "preco",                "TEXT DEFAULT ''"),
            ("historico",  "desconto",             "INTEGER DEFAULT 0"),
            ("stats",      "total_semana",         "INTEGER DEFAULT 0"),
            ("stats",      "total_mes",            "INTEGER DEFAULT 0"),
            ("stats",      "semana_inicio",        "TEXT DEFAULT ''"),
            ("pagamentos", "plano",                "TEXT DEFAULT 'mensal'"),
        ]
        async def _do():
            async with self._pool.acquire() as conn:
                for tabela, col, tipo in migracoes:
                    exists = await conn.fetchval("""
                        SELECT column_name FROM information_schema.columns
                        WHERE table_name=$1 AND column_name=$2
                    """, tabela, col)
                    if not exists:
                        try:
                            await conn.execute(
                                f"ALTER TABLE {tabela} ADD COLUMN {col} {tipo}")
                            logger.info(f"✅ Migração: '{col}' adicionado em '{tabela}'")
                        except Exception as e:
                            logger.warning(f"Migração '{col}' em '{tabela}': {e}")
        self._run(_do())

    # ════════════════════════════════════════════════════════════
    #  ASSINANTES
    # ════════════════════════════════════════════════════════════
    def get_assinante(self, uid: int) -> Optional[Dict]:
        row = self._exec("SELECT * FROM assinantes WHERE id=%s", (uid,), fetch="one")
        if not row: return None
        d = dict(row)
        for f in ("canais_tg","canais_tg_ativos","grupos_wa","grupos_wa_ativos",
                  "blacklist_lojas","blacklist_produtos","templates_custom"):
            d[f] = self._parse_json_list(d.get(f))
        d["ativo"]      = bool(d.get("ativo", 1))
        d["vencimento"] = self._parse_date(d.get("vencimento", ""))
        return d

    def assinatura_ativa(self, uid: int) -> bool:
        a = self.get_assinante(uid)
        if not a: return False
        # ativo pode ser False mesmo com vencimento futuro (admin desativou)
        if not a.get("ativo", True): return False
        venc = self._parse_date(a.get("vencimento") or "")
        if not venc: return False
        return venc >= datetime.now().strftime("%Y-%m-%d")

    def get_limite_plano(self, uid: int, campo: str) -> Any:
        a = self.get_assinante(uid)
        plano = (a.get("plano") or "mensal") if a else "mensal"
        plano_cfg = PLANOS.get(plano, PLANOS["mensal"])
        return plano_cfg.get(campo, PLANOS["mensal"].get(campo))

    def plano_permite(self, uid: int, recurso: str) -> bool:
        a = self.get_assinante(uid)
        plano = (a.get("plano") or "mensal") if a else "mensal"
        return bool(PLANOS.get(plano, PLANOS["mensal"]).get(recurso, False))

    def ativar(self, uid: int, dias: int = 30, plano: str = "mensal",
               email: str = None, nome: str = None, username: str = None) -> str:
        venc   = (datetime.now() + timedelta(days=dias)).strftime("%Y-%m-%d")
        limite = PLANOS.get(plano, PLANOS["mensal"]).get("canais_tg", 10)
        self._exec("""
            INSERT INTO assinantes
              (id,vencimento,ativo,ativado_em,plano,email,nome,username,limite_canais)
            VALUES (%s,%s,1,%s,%s,%s,%s,%s,%s)
            ON CONFLICT(id) DO UPDATE SET
              vencimento=EXCLUDED.vencimento, ativo=1, plano=EXCLUDED.plano,
              limite_canais=EXCLUDED.limite_canais,
              email=COALESCE(EXCLUDED.email, assinantes.email),
              nome=COALESCE(EXCLUDED.nome, assinantes.nome),
              username=COALESCE(EXCLUDED.username, assinantes.username)
        """, (uid, venc, datetime.now().isoformat(), plano, email, nome, username, limite))
        self._exec("INSERT INTO stats(user_id) VALUES(%s) ON CONFLICT DO NOTHING", (uid,))
        return venc

    def desativar(self, uid: int):
        self._exec("UPDATE assinantes SET ativo=0, modo_auto=0 WHERE id=%s", (uid,))

    def listar_assinantes(self) -> List[Dict]:
        rows = self._exec("SELECT * FROM assinantes WHERE ativo=1", fetch="all") or []
        result = []
        for r in rows:
            d = dict(r)
            for f in ("canais_tg","canais_tg_ativos","grupos_wa","grupos_wa_ativos"):
                d[f] = self._parse_json_list(d.get(f))
            result.append(d)
        return result

    def desativar_expirados(self) -> int:
        """
        Desativa assinantes expirados usando comparação Python-side
        para lidar com qualquer formato de data (YYYY-MM, YYYY-MM-DD, etc.)
        Margem de 1 dia para evitar falsos positivos.
        """
        hoje    = datetime.now().date()
        margem  = timedelta(days=1)
        limite  = (hoje - margem).strftime("%Y-%m-%d")
        # Busca todos ativos com vencimento que possa estar expirado
        rows = self._exec(
            "SELECT id, vencimento FROM assinantes WHERE ativo=1",
            fetch="all") or []
        n = 0
        for row in rows:
            venc_str = Database._parse_date(row.get("vencimento") or "")
            if not venc_str:
                continue
            try:
                if datetime.strptime(venc_str, "%Y-%m-%d").date() < (hoje - margem):
                    self.desativar(row["id"])
                    n += 1
            except Exception:
                continue
        if n:
            logger.info(f"[DB] {n} assinante(s) expirado(s) desativado(s)")
        return n

    # ── onboarding ────────────────────────────────────────────
    def get_onboarding(self, uid: int) -> int:
        row = self._exec("SELECT onboarding_step FROM assinantes WHERE id=%s", (uid,), fetch="one")
        return (row or {}).get("onboarding_step", 0)

    def set_onboarding(self, uid: int, step: int):
        self._exec("UPDATE assinantes SET onboarding_step=%s WHERE id=%s", (step, uid))

    # ── canais TG ─────────────────────────────────────────────
    def set_canais_tg(self, uid: int, canais: List[str], ativos: List[str] = None):
        if ativos is None: ativos = canais
        self._exec("UPDATE assinantes SET canais_tg=%s, canais_tg_ativos=%s WHERE id=%s",
                   (json.dumps(canais), json.dumps(ativos), uid))

    def toggle_canal_tg(self, uid: int, canal: str) -> bool:
        a = self.get_assinante(uid)
        if not a: return False
        ativos = a.get("canais_tg_ativos", [])
        if canal in ativos: ativos.remove(canal); novo = False
        else: ativos.append(canal); novo = True
        self._exec("UPDATE assinantes SET canais_tg_ativos=%s WHERE id=%s",
                   (json.dumps(ativos), uid))
        return novo

    # ── grupos WA ─────────────────────────────────────────────
    def set_grupos_wa(self, uid: int, grupos: List[str], ativos: List[str] = None,
                      nomes: Dict[str, str] = None):
        if ativos is None: ativos = grupos
        self._exec(
            "UPDATE assinantes SET grupos_wa=%s, grupos_wa_ativos=%s, grupos_wa_nomes=%s WHERE id=%s",
            (json.dumps(grupos), json.dumps(ativos),
             json.dumps(nomes or {}, ensure_ascii=False), uid))

    def toggle_grupo_wa(self, uid: int, grupo: str) -> bool:
        a = self.get_assinante(uid)
        if not a: return False
        ativos = a.get("grupos_wa_ativos", [])
        if grupo in ativos: ativos.remove(grupo); novo = False
        else: ativos.append(grupo); novo = True
        self._exec("UPDATE assinantes SET grupos_wa_ativos=%s WHERE id=%s",
                   (json.dumps(ativos), uid))
        return novo

    def get_nomes_grupos_wa(self, uid: int) -> Dict[str, str]:
        row = self._exec("SELECT grupos_wa_nomes FROM assinantes WHERE id=%s", (uid,), fetch="one")
        try:    return json.loads((row or {}).get("grupos_wa_nomes") or "{}") 
        except: return {}

    def set_wa_bridge(self, uid: int, bridge_url: str):
        self._exec("UPDATE assinantes SET wa_bridge_url=%s WHERE id=%s", (bridge_url, uid))

    # ── nichos TG ─────────────────────────────────────────────
    def get_nichos_tg(self, uid: int) -> Dict[str, List[str]]:
        row = self._exec("SELECT nichos_tg FROM assinantes WHERE id=%s", (uid,), fetch="one")
        try:
            raw = json.loads((row or {}).get("nichos_tg") or "{}")
            return {k: v if isinstance(v, list) else ([v] if (isinstance(v,str) and v and v!="todos") else [])
                    for k,v in raw.items()}
        except: return {}

    def set_nichos_tg(self, uid: int, canal: str, nichos: List[str]) -> None:
        dados = self.get_nichos_tg(uid)
        limpos = [n for n in nichos if n and n != "todos"]
        if limpos: dados[canal] = limpos
        else: dados.pop(canal, None)
        self._exec("UPDATE assinantes SET nichos_tg=%s WHERE id=%s", (json.dumps(dados), uid))

    def set_nicho_tg(self, uid: int, canal: str, nicho: str) -> None:
        dados = self.get_nichos_tg(uid); atual = dados.get(canal, [])
        if nicho == "todos": dados.pop(canal, None)
        elif nicho in atual:
            atual.remove(nicho)
            if atual: dados[canal] = atual
            else: dados.pop(canal, None)
        else: atual.append(nicho); dados[canal] = atual
        self._exec("UPDATE assinantes SET nichos_tg=%s WHERE id=%s", (json.dumps(dados), uid))

    # ── nichos WA ─────────────────────────────────────────────
    def get_nichos_wa(self, uid: int) -> Dict[str, List[str]]:
        row = self._exec("SELECT nichos_wa FROM assinantes WHERE id=%s", (uid,), fetch="one")
        try:
            raw = json.loads((row or {}).get("nichos_wa") or "{}")
            return {k: v if isinstance(v, list) else ([v] if (isinstance(v,str) and v and v!="todos") else [])
                    for k,v in raw.items()}
        except: return {}

    def set_nichos_wa(self, uid: int, grupo: str, nichos: List[str]) -> None:
        dados = self.get_nichos_wa(uid)
        limpos = [n for n in nichos if n and n != "todos"]
        if limpos: dados[grupo] = limpos
        else: dados.pop(grupo, None)
        self._exec("UPDATE assinantes SET nichos_wa=%s WHERE id=%s", (json.dumps(dados), uid))

    def set_nicho_wa(self, uid: int, grupo: str, nicho: str) -> None:
        dados = self.get_nichos_wa(uid); atual = dados.get(grupo, [])
        if nicho == "todos": dados.pop(grupo, None)
        elif nicho in atual:
            atual.remove(nicho)
            if atual: dados[grupo] = atual
            else: dados.pop(grupo, None)
        else: atual.append(nicho); dados[grupo] = atual
        self._exec("UPDATE assinantes SET nichos_wa=%s WHERE id=%s", (json.dumps(dados), uid))

    # ── templates por canal ───────────────────────────────────
    def get_templates_tg(self, uid: int) -> Dict[str, int]:
        row = self._exec("SELECT templates_tg FROM assinantes WHERE id=%s", (uid,), fetch="one")
        try:    return json.loads((row or {}).get("templates_tg") or "{}")
        except: return {}

    def set_template_canal_tg(self, uid: int, canal: str, idx: int):
        t = self.get_templates_tg(uid)
        if idx < 0: t.pop(canal, None)
        else: t[canal] = idx
        self._exec("UPDATE assinantes SET templates_tg=%s WHERE id=%s", (json.dumps(t), uid))

    def get_templates_wa(self, uid: int) -> Dict[str, int]:
        row = self._exec("SELECT templates_wa FROM assinantes WHERE id=%s", (uid,), fetch="one")
        try:    return json.loads((row or {}).get("templates_wa") or "{}")
        except: return {}

    def set_template_grupo_wa(self, uid: int, grupo: str, idx: int):
        t = self.get_templates_wa(uid)
        if idx < 0: t.pop(grupo, None)
        else: t[grupo] = idx
        self._exec("UPDATE assinantes SET templates_wa=%s WHERE id=%s", (json.dumps(t), uid))

    def get_template_ativo(self, uid: int) -> int:
        row = self._exec("SELECT template_ativo_idx FROM assinantes WHERE id=%s", (uid,), fetch="one")
        return (row or {}).get("template_ativo_idx", -1)

    def set_template_ativo(self, uid: int, idx: int):
        self._exec("UPDATE assinantes SET template_ativo_idx=%s WHERE id=%s", (idx, uid))

    def set_estilo(self, uid: int, estilo: str):
        self._exec("UPDATE assinantes SET estilo=%s WHERE id=%s", (estilo, uid))

    def set_modo_auto(self, uid: int, ativo: bool, min_desconto: int = 20):
        self._exec("UPDATE assinantes SET modo_auto=%s, min_desconto=%s WHERE id=%s",
                   (1 if ativo else 0, min_desconto, uid))

    def listar_auto_ativos(self) -> List[Dict]:
        rows = self._exec("SELECT * FROM assinantes WHERE ativo=1 AND modo_auto=1", fetch="all") or []
        result = []
        for r in rows:
            d = dict(r)
            for f in ("canais_tg","canais_tg_ativos","grupos_wa","grupos_wa_ativos"):
                d[f] = self._parse_json_list(d.get(f))
            d["vencimento"] = self._parse_date(d.get("vencimento",""))
            result.append(d)
        return result

    def registrar_auto_post_ts(self, uid: int):
        self._exec("UPDATE assinantes SET ultimo_auto_post=%s WHERE id=%s",
                   (datetime.now().isoformat(), uid))

    def pode_auto_postar(self, uid: int) -> bool:
        a = self.get_assinante(uid)
        if not a: return False
        ultimo = a.get("ultimo_auto_post","")
        if not ultimo: return True
        try:
            diff = (datetime.now() - datetime.fromisoformat(ultimo)).total_seconds()
            return diff >= (cfg.AUTO_POSTER_INTERVALO - 5) * 60
        except: return True

    # ── blacklist ─────────────────────────────────────────────
    def add_blacklist_loja(self, uid: int, loja: str):
        a = self.get_assinante(uid)
        if not a: return
        bl = a.get("blacklist_lojas",[])
        if loja.lower() not in [x.lower() for x in bl]:
            bl.append(loja)
            self._exec("UPDATE assinantes SET blacklist_lojas=%s WHERE id=%s",
                       (json.dumps(bl), uid))

    def remove_blacklist_loja(self, uid: int, loja: str):
        a = self.get_assinante(uid)
        if not a: return
        bl = [x for x in a.get("blacklist_lojas",[]) if x.lower() != loja.lower()]
        self._exec("UPDATE assinantes SET blacklist_lojas=%s WHERE id=%s",
                   (json.dumps(bl), uid))

    def em_blacklist(self, uid: int, produto) -> bool:
        a = self.get_assinante(uid)
        if not a: return False
        bl_l = [x.lower() for x in a.get("blacklist_lojas",[])]
        bl_p = [x.lower() for x in a.get("blacklist_produtos",[])]
        if any(b in (produto.loja or "").lower() for b in bl_l if b): return True
        if any(b in (produto.titulo or "").lower() for b in bl_p if b): return True
        return False

    # ── templates custom ──────────────────────────────────────
    def get_templates_custom(self, uid: int) -> List[Dict]:
        a = self.get_assinante(uid)
        return a.get("templates_custom",[]) if a else []

    def add_template_custom(self, uid: int, nome: str, template: str) -> bool:
        limite = self.get_limite_plano(uid, "templates_custom")
        templates = self.get_templates_custom(uid)
        if len(templates) >= limite: return False
        templates.append({"nome": nome, "template": template})
        self._exec("UPDATE assinantes SET templates_custom=%s WHERE id=%s",
                   (json.dumps(templates, ensure_ascii=False), uid))
        return True

    def remove_template_custom(self, uid: int, idx: int):
        templates = self.get_templates_custom(uid)
        if 0 <= idx < len(templates):
            templates.pop(idx)
            self._exec("UPDATE assinantes SET templates_custom=%s WHERE id=%s",
                       (json.dumps(templates, ensure_ascii=False), uid))

    # ── afiliados ─────────────────────────────────────────────
    def set_aff_code(self, uid: int, plataforma: str, codigo: str):
        if plataforma not in _AFF_COLS_PERMITIDAS:
            raise ValueError(f"Plataforma inválida: {plataforma}")
        self._exec(f"UPDATE assinantes SET aff_{plataforma}=%s WHERE id=%s", (codigo, uid))

    def get_aff_code(self, uid: int, plataforma: str) -> str:
        if plataforma not in _AFF_COLS_PERMITIDAS: return ""
        row = self._exec(f"SELECT aff_{plataforma} FROM assinantes WHERE id=%s",
                         (uid,), fetch="one")
        return (row or {}).get(f"aff_{plataforma}") or ""

    # ── bot chats ─────────────────────────────────────────────
    def registrar_bot_chat(self, chat_id: str, titulo: str, tipo: str):
        try:
            self._exec("""
                INSERT INTO bot_chats (chat_id,titulo,tipo,criado_em)
                VALUES (%s,%s,%s,%s)
                ON CONFLICT(chat_id) DO UPDATE SET titulo=EXCLUDED.titulo, tipo=EXCLUDED.tipo
            """, (str(chat_id), titulo or str(chat_id), tipo, datetime.now().isoformat()))
        except Exception as e: logger.warning(f"registrar_bot_chat: {e}")

    def remover_bot_chat(self, chat_id: str):
        try: self._exec("DELETE FROM bot_chats WHERE chat_id=%s", (str(chat_id),))
        except: pass

    def listar_bot_chats(self) -> List[Dict]:
        try:
            rows = self._exec("SELECT * FROM bot_chats ORDER BY titulo", fetch="all") or []
            return [dict(r) for r in rows]
        except: return []

    # ── testes ────────────────────────────────────────────────
    def usou_teste(self, uid: int) -> bool:
        return bool(self._exec("SELECT 1 FROM testes WHERE user_id=%s", (uid,), fetch="one"))

    def ativar_teste(self, uid: int) -> str:
        fim = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d")
        self._exec("""
            INSERT INTO testes VALUES (%s,%s,%s)
            ON CONFLICT(user_id) DO UPDATE SET inicio=EXCLUDED.inicio, fim=EXCLUDED.fim
        """, (uid, datetime.now().isoformat(), fim))
        self.ativar(uid, 7, "teste")
        return fim

    # ── links ─────────────────────────────────────────────────
    def salvar_link(self, uid: int, url: str, titulo: str, plataforma: str,
                    url_hash: str, preco: str = "", imagem: str = "", video: str = "") -> int:
        async def _do():
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow(_to_pg("""
                    INSERT INTO links
                      (user_id,url,titulo,plataforma,preco,imagem,url_hash,video,criado_em)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT(url_hash) DO UPDATE SET
                      titulo=EXCLUDED.titulo, preco=EXCLUDED.preco, imagem=EXCLUDED.imagem
                    RETURNING id
                """), uid, url, titulo, plataforma, preco, imagem,
                    url_hash, video, datetime.now().isoformat())
                return row["id"] if row else 0
        try:    return self._run(_do())
        except: return 0

    def get_link(self, link_id: int, uid: int) -> Optional[Dict]:
        row = self._exec("SELECT * FROM links WHERE id=%s AND user_id=%s",
                         (link_id, uid), fetch="one")
        return dict(row) if row else None

    def listar_links(self, uid: int, limite: int = 50) -> List[Dict]:
        rows = self._exec(
            "SELECT * FROM links WHERE user_id=%s ORDER BY criado_em DESC LIMIT %s",
            (uid, limite), fetch="all") or []
        return [dict(r) for r in rows]

    def remover_link(self, link_id: int, uid: int) -> bool:
        async def _do():
            async with self._pool.acquire() as conn:
                r = await conn.execute(
                    _to_pg("DELETE FROM links WHERE id=%s AND user_id=%s"), link_id, uid)
                return r.split()[-1] != "0"
        try:    return self._run(_do())
        except: return False

    # ── agendamentos ──────────────────────────────────────────
    def agendar(self, uid: int, link_id: int, url: str, url_hash: str,
                titulo: str, canal: str, horario: str, destinos: str = "telegram"):
        try:
            self._exec("""
                INSERT INTO agendamentos
                  (user_id,link_id,url,url_hash,titulo,canal,horario,destinos,criado_em,status)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'pendente')
                ON CONFLICT(user_id,url_hash,horario) DO NOTHING
            """, (uid, link_id, url, url_hash, titulo, canal, horario, destinos,
                  datetime.now().isoformat()))
        except: pass

    def pendentes(self, horario: str) -> List[Dict]:
        """
        Busca agendamentos pendentes para o horário atual.
        Usa janela de ±2 minutos para não perder por diferença de segundos.
        """
        try:
            from datetime import datetime as _dt, timedelta as _td
            agora  = _dt.strptime(horario, "%H:%M")
            inicio = (agora - _td(minutes=2)).strftime("%H:%M")
            fim    = (agora + _td(minutes=1)).strftime("%H:%M")
            rows = self._exec("""
                SELECT * FROM agendamentos
                WHERE horario BETWEEN %s AND %s
                  AND status='pendente'
                ORDER BY horario
            """, (inicio, fim), fetch="all") or []
            return [dict(r) for r in rows]
        except Exception as e:
            logger.error(f"[DB] pendentes: {e}")
            return []

    def marcar_postado(self, aid: int):
        self._exec("UPDATE agendamentos SET status='postado', postado_em=%s WHERE id=%s",
                   (datetime.now().isoformat(), aid))

    def listar_agendamentos(self, uid: int) -> List[Dict]:
        rows = self._exec(
            "SELECT * FROM agendamentos WHERE user_id=%s AND status='pendente' ORDER BY horario",
            (uid,), fetch="all") or []
        return [dict(r) for r in rows]

    def cancelar_agendamento(self, aid: int, uid: int) -> bool:
        async def _do():
            async with self._pool.acquire() as conn:
                r = await conn.execute(
                    _to_pg("DELETE FROM agendamentos WHERE id=%s AND user_id=%s"), aid, uid)
                return r.split()[-1] != "0"
        try:    return self._run(_do())
        except: return False

    # ── historico ─────────────────────────────────────────────
    def log_postagem(self, uid: int, url_hash: str, canal: str,
                     ok: bool, erro: str = "", metodo: str = "", destino: str = "telegram",
                     url: str = "", titulo: str = "", imagem: str = "",
                     preco: str = "", desconto: int = 0, video: str = ""):
        try:
            self._exec("""
                INSERT INTO historico
                  (user_id,hash,url,titulo,imagem,canal,destino,
                   postado_em,status,erro,metodo,preco,desconto)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (uid, url_hash, url, titulo, imagem, canal, destino,
                  datetime.now().isoformat(), "sucesso" if ok else "erro",
                  erro, metodo, preco, desconto))
        except Exception as e:
            logger.warning(f"log_postagem: {e}")

    def listar_historico(self, uid: int, limite: int = 20) -> List[Dict]:
        rows = self._exec("""
            SELECT * FROM historico WHERE user_id=%s AND status='sucesso'
            ORDER BY postado_em DESC LIMIT %s
        """, (uid, limite), fetch="all") or []
        return [dict(r) for r in rows]

    def stats_historico_semana(self, uid: int) -> Dict:
        sete_dias = (datetime.now() - timedelta(days=7)).isoformat()
        row = self._exec("""
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN destino='telegram' THEN 1 ELSE 0 END) as tg,
                   SUM(CASE WHEN destino='whatsapp' THEN 1 ELSE 0 END) as wa
            FROM historico WHERE user_id=%s AND postado_em>=%s AND status='sucesso'
        """, (uid, sete_dias), fetch="one")
        return dict(row) if row else {"total":0,"tg":0,"wa":0}

    def stats_historico_por_dia(self, uid: int, dias: int = 7) -> List[Dict]:
        inicio = (datetime.now() - timedelta(days=dias)).isoformat()
        rows = self._exec("""
            SELECT DATE(postado_em) as dia, COUNT(*) as total
            FROM historico WHERE user_id=%s AND postado_em>=%s AND status='sucesso'
            GROUP BY dia ORDER BY dia
        """, (uid, inicio), fetch="all") or []
        return [dict(r) for r in rows]

    # ── auto_postagens ────────────────────────────────────────
    def ja_auto_postou(self, uid: int, item_hash: str) -> bool:
        return bool(self._exec(
            "SELECT 1 FROM auto_postagens WHERE user_id=%s AND item_hash=%s",
            (uid, item_hash), fetch="one"))

    def registrar_auto_postagem(self, uid: int, item_hash: str,
                                titulo: str, preco: str, desconto: int):
        try:
            self._exec("""
                INSERT INTO auto_postagens
                  (user_id,item_hash,titulo,preco,desconto,postado_em)
                VALUES (%s,%s,%s,%s,%s,%s)
                ON CONFLICT(user_id,item_hash) DO NOTHING
            """, (uid, item_hash, titulo, preco, desconto, datetime.now().isoformat()))
        except: pass

    def limpar_auto_postagens_antigas(self, uid: int, dias: int = 30):
        limite = (datetime.now() - timedelta(days=dias)).isoformat()
        self._exec("DELETE FROM auto_postagens WHERE user_id=%s AND postado_em<%s",
                   (uid, limite))

    # ── stats ─────────────────────────────────────────────────
    def inc_postagem(self, uid: int, destino: str = "telegram") -> None:
        try:
            now_str = datetime.now().isoformat()
            col = "total_wa" if destino == "whatsapp" else "total_postagens"

            # Garante que a linha existe
            self._exec(
                "INSERT INTO stats(user_id, total_postagens, total_wa, total_semana, "
                "total_mes, semana_inicio) VALUES(%s,0,0,0,0,'') ON CONFLICT DO NOTHING",
                (uid,))

            # Lê semana_inicio atual
            row = self._exec(
                "SELECT semana_inicio, ultima_postagem FROM stats WHERE user_id=%s",
                (uid,), fetch="one") or {}
            semana_ini = (row.get("semana_inicio") or "").strip()

            nova_semana = False
            if semana_ini:
                try:
                    diff = (datetime.now() - datetime.fromisoformat(semana_ini)).days
                    nova_semana = diff >= 7
                except Exception:
                    nova_semana = True
            else:
                nova_semana = True

            if nova_semana:
                self._exec(
                    "UPDATE stats SET total_semana=0, semana_inicio=%s WHERE user_id=%s",
                    (now_str, uid))

            # Incrementa contadores
            self._exec(
                f"UPDATE stats SET {col}={col}+1, total_semana=total_semana+1,"
                f" total_mes=total_mes+1, ultima_postagem=%s WHERE user_id=%s",
                (now_str, uid))

        except Exception as e:
            logger.error(f"[DB] inc_postagem uid={uid} destino={destino}: {e}")

    def get_stats(self, uid: int) -> Dict:
        try:
            row = self._exec("SELECT * FROM stats WHERE user_id=%s", (uid,), fetch="one")
            if not row:
                return {"total_postagens": 0, "total_wa": 0,
                        "total_semana": 0, "total_mes": 0, "ultima_postagem": None}
            d = dict(row)
            # Garante inteiros onde NULL pode ter entrado
            for col in ("total_postagens", "total_wa", "total_semana", "total_mes"):
                d[col] = int(d.get(col) or 0)
            return d
        except Exception as e:
            logger.error(f"[DB] get_stats uid={uid}: {e}")
            return {"total_postagens": 0, "total_wa": 0,
                    "total_semana": 0, "total_mes": 0, "ultima_postagem": None}

    # ── email / pagamentos ─────────────────────────────────────
    def salvar_email(self, email: str, uid: int):
        self._exec("""
            INSERT INTO emails VALUES(%s,%s,%s)
            ON CONFLICT(email) DO UPDATE SET user_id=EXCLUDED.user_id
        """, (email.lower().strip(), uid, datetime.now().isoformat()))

    def buscar_uid_por_email(self, email: str) -> Optional[int]:
        row = self._exec("SELECT user_id FROM emails WHERE email=%s",
                         (email.lower().strip(),), fetch="one")
        return (row or {}).get("user_id")

    def log_pagamento(self, uid: int, order_id: str, status: str,
                      valor: float, metodo: str, email: str = "", plano: str = "mensal"):
        try:
            self._exec("""
                INSERT INTO pagamentos
                  (user_id,order_id,status,valor,email,metodo,plano,criado_em)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT(order_id) DO NOTHING
            """, (uid, order_id, status, valor, email, metodo, plano,
                  datetime.now().isoformat()))
        except: pass

    def pgto_processado(self, order_id: str) -> bool:
        row = self._exec("SELECT processado FROM pagamentos WHERE order_id=%s",
                         (order_id,), fetch="one")
        return bool((row or {}).get("processado"))

    def marcar_pgto(self, order_id: str):
        self._exec("UPDATE pagamentos SET processado=1 WHERE order_id=%s", (order_id,))

    # ── referrals ─────────────────────────────────────────────
    def salvar_referral(self, ref_id: int, new_id: int):
        try:
            self._exec("""
                INSERT INTO referrals(referrer_id,referred_id,criado_em,status)
                VALUES(%s,%s,%s,'pendente')
                ON CONFLICT(referred_id) DO NOTHING
            """, (ref_id, new_id, datetime.now().isoformat()))
        except: pass

    def processar_referral(self, new_id: int) -> Optional[int]:
        row = self._exec(
            "SELECT * FROM referrals WHERE referred_id=%s AND status='pendente'",
            (new_id,), fetch="one")
        if not row: return None
        ref_id = row["referrer_id"]
        a = self.get_assinante(ref_id)
        if a:
            nova = (datetime.strptime(a["vencimento"],"%Y-%m-%d") +
                    timedelta(days=cfg.BONUS_CONVIDANTE)).strftime("%Y-%m-%d")
            self._exec("UPDATE assinantes SET vencimento=%s WHERE id=%s", (nova, ref_id))
        else:
            self.ativar(ref_id, cfg.BONUS_CONVIDANTE, "referral")
        a2 = self.get_assinante(new_id)
        if a2:
            nova2 = (datetime.strptime(a2["vencimento"],"%Y-%m-%d") +
                     timedelta(days=cfg.BONUS_CONVIDADO)).strftime("%Y-%m-%d")
            self._exec("UPDATE assinantes SET vencimento=%s WHERE id=%s", (nova2, new_id))
        self._exec("UPDATE referrals SET status='recompensado' WHERE referred_id=%s", (new_id,))
        return ref_id

    def stats_referral(self, uid: int) -> Dict:
        t = (self._exec("SELECT COUNT(*) as n FROM referrals WHERE referrer_id=%s AND status='recompensado'",
                        (uid,), fetch="one") or {}).get("n", 0)
        p = (self._exec("SELECT COUNT(*) as n FROM referrals WHERE referrer_id=%s AND status='pendente'",
                        (uid,), fetch="one") or {}).get("n", 0)
        return {"total": t, "pendentes": p}

    # ── copies custom ─────────────────────────────────────────
    def salvar_copy_custom(self, uid: int, url_hash: str, copy: str):
        try:
            self._exec("""
                INSERT INTO copies_custom(user_id,url_hash,copy,criado_em)
                VALUES(%s,%s,%s,%s)
                ON CONFLICT(user_id,url_hash)
                DO UPDATE SET copy=EXCLUDED.copy, criado_em=EXCLUDED.criado_em
            """, (uid, url_hash, copy, datetime.now().isoformat()))
        except Exception as e: logger.warning(f"salvar_copy_custom: {e}")

    def get_copy_custom(self, uid: int, url_hash: str) -> Optional[str]:
        try:
            row = self._exec(
                "SELECT copy FROM copies_custom WHERE user_id=%s AND url_hash=%s",
                (uid, url_hash), fetch="one")
            return (row or {}).get("copy")
        except: return None

    def limpar_copy_custom(self, uid: int, url_hash: str):
        try: self._exec(
            "DELETE FROM copies_custom WHERE user_id=%s AND url_hash=%s", (uid, url_hash))
        except: pass

    # ── notificações ──────────────────────────────────────────
    def notif(self, tipo: str, uid: int, msg: str):
        try:
            self._exec(
                "INSERT INTO notificacoes(tipo,user_id,mensagem,criado_em) VALUES(%s,%s,%s,%s)",
                (tipo, uid, msg, datetime.now().isoformat()))
        except: pass

    def notifs_pendentes(self) -> List[Dict]:
        rows = self._exec(
            "SELECT * FROM notificacoes WHERE lida=0 ORDER BY criado_em DESC LIMIT 30",
            fetch="all") or []
        return [dict(r) for r in rows]

    # ── vencimentos ───────────────────────────────────────────
    def assinantes_vencendo(self, dias: int = 3) -> List[Dict]:
        data_limite = (datetime.now() + timedelta(days=dias)).strftime("%Y-%m-%d")
        hoje        = datetime.now().strftime("%Y-%m-%d")
        rows = self._exec("""
            SELECT * FROM assinantes WHERE ativo=1 AND vencimento::text BETWEEN %s AND %s
        """, (hoje, data_limite), fetch="all") or []
        result = []
        for r in rows:
            d = dict(r)
            for f in ("canais_tg","canais_tg_ativos","grupos_wa","grupos_wa_ativos"):
                d[f] = self._parse_json_list(d.get(f))
            result.append(d)
        return result

    # ── admin overview ────────────────────────────────────────
    def admin_overview(self) -> Dict:
        def _q(sql, params=()):
            row = self._exec(sql, params, fetch="one") or {}
            return list(row.values())[0] if row else 0
        return {
            "assinantes": _q("SELECT COUNT(*) FROM assinantes WHERE ativo=1"),
            "testes":     _q("SELECT COUNT(*) FROM testes"),
            "postagens":  _q("SELECT COALESCE(SUM(total_postagens),0) FROM stats"),
            "wa_posts":   _q("SELECT COALESCE(SUM(total_wa),0) FROM stats"),
            "vendas":     _q("SELECT COUNT(*) FROM pagamentos WHERE status='paid'"),
            "receita":    _q("SELECT COALESCE(SUM(valor),0.0) FROM pagamentos WHERE status='paid'"),
            "emails":     _q("SELECT COUNT(*) FROM emails"),
            "links":      _q("SELECT COUNT(*) FROM links"),
            "referrals":  _q("SELECT COUNT(*) FROM referrals WHERE status='recompensado'"),
            "auto_posts": _q("SELECT COUNT(DISTINCT item_hash) FROM auto_postagens"),
            "hoje":       _q("SELECT COUNT(*) FROM historico WHERE DATE(postado_em)=CURRENT_DATE AND status='sucesso'"),
        }

db = Database()
def criar_link_pagamento(uid: int, nome: str = "", email: str = "", telefone: str = "") -> str:
    """
    Cria preferência de pagamento dinâmica com o UID no external_reference.
    IMPORTANTE: Usa preference (pagamento único) em vez de assinatura,
    pois assinaturas MP ignoram external_reference passado na URL.
    """
    # Fallback: link fixo com UID embutido no external_reference via query string
    # (funciona para preference, não para preapproval/assinatura)
    FALLBACK = f"https://www.mercadopago.com.br/checkout/v1/redirect?pref_id=325834470-73df59f2-8b3e-48ae-a115-f22af6b0eba1&external_reference={uid}"

    if not cfg.MP_ACCESS_TOKEN:
        logger.warning("[MP] MP_ACCESS_TOKEN não configurado, usando fallback")
        return FALLBACK

    sdk = mercadopago.SDK(cfg.MP_ACCESS_TOKEN)

    primeiro_nome = (nome.split()[0] if nome else "Cliente")
    sobrenome     = (" ".join(nome.split()[1:]) if len(nome.split()) > 1 else "Bot")

    preference_data = {
        "items": [{
            "id":          "assinatura_bot_mensal",
            "title":       "Bot Afiliados PRO — 30 dias",
            "description": "Acesso completo ao bot por 30 dias",
            "category_id": "services",
            "quantity":    1,
            "currency_id": "BRL",
            "unit_price":  19.99,
        }],
        "payer": {
            "email":      email or f"bot_{uid}@pagamento.com",
            "first_name": primeiro_nome,
            "last_name":  sobrenome,
        },
        # UID do Telegram — chave para ativar a assinatura no webhook
        "external_reference": str(uid),
        "metadata": {
            "uid":         str(uid),
            "telegram_id": str(uid),
        },
        "notification_url": "https://botautomacao.vercel.app/webhook/mercadopago",
        "back_urls": {
            "success": f"https://t.me/oferta_2026_bot?start=pago_{uid}",
            "failure": f"https://t.me/oferta_2026_bot",
            "pending": f"https://t.me/oferta_2026_bot",
        },
        "auto_return":          "approved",
        "binary_mode":          False,
        "statement_descriptor": "BOT AFILIADOS",
        "payment_methods": {
            "excluded_payment_types": [],
            "installments":           12,
        },
    }

    try:
        preference = sdk.preference().create(preference_data)
        if preference.get("status") == 201:
            link = preference["response"].get("init_point", "")
            logger.info(f"[MP] Preferência criada para uid={uid}: {link[:60]}")
            return link
        else:
            logger.error(f"[MP] Erro ao criar preferência: {preference.get('response','')}")
            return FALLBACK
    except Exception as e:
        logger.error(f"[MP] Exceção ao criar preferência: {e}")
        return FALLBACK
    
    # ══════════════════════════════════════════════════════════════
    #  NOTIFICADOR ADMIN
    # ══════════════════════════════════════════════════════════════
class Notif:
        @staticmethod
        def _bg(coro):
            if telegram_app and _main_loop and _main_loop.is_running():
                asyncio.run_coroutine_threadsafe(coro, _main_loop)
    
        @staticmethod
        async def nova_assinatura(uid: int, plano: str, venc: str,
                                  valor: float = None, email: str = None, metodo: str = "manual"):
            plano_info = PLANOS.get(plano, {})
            txt = (f"💎 <b>NOVA ASSINATURA</b>\n\n"
                   f"👤 ID: <code>{uid}</code>\n"
                   f"📋 Plano: <b>{plano_info.get('nome', plano.upper())}</b>\n"
                   f"📅 Vencimento: <b>{venc}</b>\n"
                   + (f"💰 Valor: R${valor:.2f}\n" if valor else "")
                   + (f"📧 Email: <code>{email}</code>\n" if email else "")
                   + f"🔧 Método: {metodo}")
            db.notif("assinatura", uid, f"Plano:{plano} Venc:{venc}")
            if telegram_app:
                try: await telegram_app.bot.send_message(cfg.ID_ADMIN, txt, parse_mode="HTML")
                except Exception: pass
    
        @staticmethod
        async def novo_email(uid: int, email: str, username: str = None):
            txt = (f"📧 <b>NOVO EMAIL</b>\n\n"
                   f"👤 @{username or 'N/A'} (<code>{uid}</code>)\n"
                   f"📧 Email: <code>{email}</code>")
            db.notif("email", uid, email)
            if telegram_app:
                try: await telegram_app.bot.send_message(cfg.ID_ADMIN, txt, parse_mode="HTML")
                except Exception: pass
    
        @staticmethod
        def webhook_sync(order_id: str, uid: int, status: str, plano: str = "mensal"):
            txt = (f"📩 <b>WEBHOOK</b>\n\n"
                   f"🔖 Order: <code>{order_id}</code>\n"
                   f"👤 User: <code>{uid}</code>\n"
                   f"📋 Plano: <b>{plano}</b>\n"
                   f"📌 Status: {status}")
            if telegram_app and _main_loop:
                Notif._bg(telegram_app.bot.send_message(
                    cfg.ID_ADMIN, txt, parse_mode="HTML", disable_notification=True))
    
        @staticmethod
        def assinatura_sync(uid, plano, venc, valor=None, email=None, metodo="webhook"):
            Notif._bg(Notif.nova_assinatura(uid, plano, venc, valor, email, metodo))
    
    
    # ══════════════════════════════════════════════════════════════
    #  AFILIADO — comissão garantida para o usuário
    # ══════════════════════════════════════════════════════════════
def aplicar_afiliado(uid: int, plataforma: str, url_original: str,
                         link_afiliado: str = "") -> str:
    """
    Aplica o código de afiliado do USUÁRIO no link do produto.
    Para Shopee, usa o padrão oficial /an_redir com affiliate_id.
    """
    codigo = db.get_aff_code(uid, plataforma)
    # Usa DEBUG para não poluir o log em ciclos de auto-poster
    logger.debug(f"[AFILIADO] uid={uid} plat={plataforma} codigo={codigo} url={url_original[:60]}")
    
    # ── Shopee: usa o padrão oficial /an_redir ──────────────────────
    if plataforma == "shopee":
        if not codigo:
            return url_original or link_afiliado or ""
    
        # Pega a URL limpa: remove tudo depois de '?'
        base = (url_original or link_afiliado).split("?")[0]
        if not base:
            return ""
    
        origem_codificada = quote(base, safe='')
        return f"https://s.shopee.com.br/an_redir?origin_link={origem_codificada}&affiliate_id={codigo}"
    
    # ── Outras plataformas (mantém a lógica original) ───────────────
    campos = {
        "amazon":       "tag",
        "mercadolivre": "matt_tool",
        "hotmart":      "sck",
        "kiwify":       "src",
        "monetizze":    "af",
        "magalu":       "partner",
        "aliexpress":   "aff",
    }
    if codigo and url_original:
        try:
            parsed = urlparse(url_original)
            query = parse_qs(parsed.query, keep_blank_values=True)
            campo = campos.get(plataforma, "ref")
            query[campo] = [codigo]
            return urlunparse(parsed._replace(query=urlencode(query, doseq=True)))
        except Exception:
            pass
    
    return link_afiliado or url_original
    # ══════════════════════════════════════════════════════════════
    #  ANALISADOR DE LINKS
    # ══════════════════════════════════════════════════════════════
class LinkAnalyzer:
        @staticmethod
        def analisar(url: str) -> LinkInfo:
            ul = url.lower()
            plataforma = "desconhecida"
            for plat, info in cfg.PLATAFORMAS.items():
                if any(d in ul for d in info["dominios"]):
                    plataforma = plat; break
            url_limpa  = LinkAnalyzer._limpar(url, plataforma)
            produto_id = LinkAnalyzer._produto_id(url, plataforma)
            eh_afiliado = any(x in ul for x in ["s.shopee","amzn.to","go.hotmart","ali.ski","meli.bz"])
            link = LinkInfo(url_original=url, url_limpa=url_limpa,
                            plataforma=plataforma, eh_afiliado=eh_afiliado, produto_id=produto_id)
            link.url_hash = hashlib.md5(url_limpa.encode()).hexdigest()[:URL_HASH_LENGTH]
            return link
    
        @staticmethod
        def _limpar(url: str, plat: str) -> str:
            p = urlparse(url); params = parse_qs(p.query)
            manter = {"shopee":["sp_atk","smtt"],"amazon":["tag"],"hotmart":["sck"]}.get(plat,[])
            filtrado = {k: v[0] for k, v in params.items()
                        if k in manter or any(x in k.lower() for x in ["aff","ref","src"])}
            return urlunparse((p.scheme,p.netloc,p.path,p.params,urlencode(filtrado),p.fragment))
    
        @staticmethod
        def _produto_id(url: str, plat: str) -> Optional[str]:
            patterns = {
                "shopee":  [r"/product/\d+/(\d+)",r"-i\.\d+\.(\d+)",r"itemid=(\d+)"],
                "amazon":  [r"/dp/([A-Z0-9]{10})",r"/gp/product/([A-Z0-9]{10})"],
                "hotmart": [r"go\.hotmart\.com/(\w+)"],
            }
            for pat in patterns.get(plat,[]):
                m = re.search(pat, url)
                if m: return m.group(1)
            return None
    
    
    # ══════════════════════════════════════════════════════════════
    #  EXTRATORES
    # ══════════════════════════════════════════════════════════════
class ExtratorBase(ABC):
        @abstractmethod
        async def extrair(self, link: LinkInfo) -> Produto: pass
    
    
class ExtratorShopeeOficialGraphQL(ExtratorBase):
        def __init__(self):
            self.client = (ShopeeAffiliateGraphQL(cfg.SHOPEE_APP_ID, cfg.SHOPEE_SECRET)
                           if cfg.SHOPEE_APP_ID and cfg.SHOPEE_SECRET else None)
    
        async def extrair(self, link: LinkInfo) -> Produto:
            if not self.client: raise Exception("Credenciais Shopee não configuradas")
            ck = f"shopee_gql_{link.url_hash}"
            if hit := cache.get(ck): return hit
            url = link.url_original
            if any(d in url for d in ["s.shopee","shopee.ee","shp.ee","br.shp.ee"]):
                url = await self.client._resolver_url(url)
            produto = await self.client.get_product_by_link(url)
            if not produto: raise Exception("API GraphQL não retornou produto")
            # Atualiza link com URL longa resolvida para garantir af= correto
            link_resolvido = dc_replace(link, url_original=url, url_limpa=url)
            produto.link   = link_resolvido
            produto.metodo = "shopee_graphql"
            cache.set(ck, produto, ttl=1800)
            return produto
    
    
class ExtratorShopeeAPI(ExtratorBase):
        async def extrair(self, link: LinkInfo) -> Produto:
            url = link.url_original
            if any(d in url for d in ["s.shopee","shopee.ee","shp.ee","br.shp.ee"]):
                url = await self._resolver(url)
            # Guarda URL longa resolvida para usar no link final
            link = link.__class__(url_original=url, url_limpa=url,
                                   url_hash=link.url_hash, plataforma=link.plataforma,
                                   eh_afiliado=link.eh_afiliado, produto_id=link.produto_id)
            item_id, shop_id = self._ids(url) or (None, None)
            if not item_id:
                item_id, shop_id = self._ids(url) or (None, None)
            if not item_id: raise ValueError("ID do produto não encontrado")
            ck = f"shopee_api_{item_id}_{shop_id}"
            if hit := cache.get(ck): return hit
            produto = await self._buscar(item_id, shop_id, link)
            if produto:
                cache.set(ck, produto, ttl=1800)
                return produto
            raise Exception("WAF Shopee bloqueando. Configure credenciais da API oficial.")
    
        async def _resolver(self, url: str) -> str:
            try:
                async with aiohttp.ClientSession() as s:
                    async with s.get(url, allow_redirects=True,
                                     timeout=aiohttp.ClientTimeout(total=8)) as r:
                        return str(r.url)
            except Exception: return url
    
        def _ids(self, url: str) -> Optional[Tuple]:
            patterns = [
                (r"/product/(\d+)/(\d+)",       lambda m: (m.group(2), m.group(1))),
                (r"-i\.(\d+)\.(\d+)",           lambda m: (m.group(2), m.group(1))),
                (r"/[^/?]+/(\d{6,})/(\d{8,})", lambda m: (m.group(2), m.group(1))),
                (r"itemid=(\d+).*shopid=(\d+)", lambda m: (m.group(1), m.group(2))),
                (r"shopid=(\d+).*itemid=(\d+)", lambda m: (m.group(2), m.group(1))),
            ]
            for p, e in patterns:
                m = re.search(p, url)
                if m: return e(m)
            return None
    
        async def _buscar(self, item_id, shop_id, link) -> Optional[Produto]:
            if not shop_id: return None
            h = {"User-Agent":"Mozilla/5.0 (Linux; Android 12; Pixel 6) AppleWebKit/537.36",
                 "Accept":"application/json","Referer":"https://shopee.com.br/"}
            urls = [
                f"https://shopee.com.br/api/v4/pdp/get_pdp?item_id={item_id}&shop_id={shop_id}",
                f"https://shopee.com.br/api/v4/item/get?itemid={item_id}&shopid={shop_id}",
            ]
            async with aiohttp.ClientSession() as session:
                for u in urls:
                    try:
                        async with session.get(u, headers=h,
                                               timeout=aiohttp.ClientTimeout(total=10)) as resp:
                            if resp.status != 200: continue
                            data = await resp.json(content_type=None)
                            item = data.get("data") or {}
                            if isinstance(item, dict) and "item" in item: item = item["item"]
                            if isinstance(item, dict) and item.get("name"):
                                return self._parse(item, link)
                    except Exception as e:
                        logger.warning(f"[ShopeeAPI] {e}")
            return None
    
        def _parse(self, item: dict, link: LinkInfo) -> Produto:
            def normalizar_preco(v) -> float:
                """
                Detecta automaticamente o formato do preço Shopee:
                - >= 100_000  → centavos×1000 (ex: 4990000 = R$49,90) → ÷100000
                - >= 1_000    → centavos normais (ex: 4990 = R$49,90) → ÷100
                - < 1_000     → já em reais
                """
                try:
                    val = float(v or 0)
                except Exception:
                    return 0.0
                if val <= 0:
                    return 0.0
                if val >= 100_000:
                    return round(val / 100_000, 2)
                if val >= 1_000:
                    return round(val / 100, 2)
                return round(val, 2)

            preco = normalizar_preco(item.get("price_min") or item.get("price", 0))
            orig  = normalizar_preco(item.get("price_before_discount", 0))
            if orig <= preco:
                orig = 0.0
            desconto = int((1 - preco / orig) * 100) if orig > preco > 0 else 0
            # Cap de segurança: desconto > 95% = dado suspeito
            if desconto > 95:
                orig = 0.0
                desconto = 0

            images = item.get("images", [])
            if images:
                img0   = images[0] if isinstance(images[0], str) else images[0].get("path", "")
                imagem = f"https://down-br.img.susercontent.com/file/{img0}"
            else:
                imagem = item.get("image", "")
                if imagem and not imagem.startswith("http"):
                    imagem = f"https://down-br.img.susercontent.com/file/{imagem}"

            rating_star  = (item.get("item_rating", {}) or {}).get("rating_star", 0) or item.get("rating_star", 0)
            rating_count = (item.get("item_rating", {}) or {}).get("rating_count", []) or item.get("rating_count", 0)
            if isinstance(rating_count, list):
                rating_count = sum(rating_count)
            vendidos  = max(item.get("historical_sold", 0), item.get("sold", 0), item.get("sold_count", 0))
            shop_info = item.get("shop_info", {}) or {}
            loja      = item.get("shop_name") or shop_info.get("shop_name") or ""

            # Vídeo da API REST
            videos = item.get("video_info_list") or item.get("videos") or []
            video_url_api = ""
            if videos and isinstance(videos, list):
                v0 = videos[0]
                if isinstance(v0, dict):
                    video_url_api = (v0.get("default_format",{}) or {}).get("url","") or \
                                    v0.get("url","") or v0.get("video_url","")
            return Produto(
                titulo=item.get("name","")[:120], descricao=(item.get("description") or "")[:400],
                preco=f"{preco:.2f}" if preco > 0 else "",
                preco_original=f"{orig:.2f}" if orig > 0 else "",
                imagem=imagem, link=link,
                avaliacao=f"{rating_star:.1f}" if rating_star else "",
                num_avaliacoes=str(rating_count) if rating_count else "",
                vendidos=str(vendidos) if vendidos else "",
                loja=loja, desconto_pct=desconto, metodo="shopee_api",
                video=video_url_api,
            )
    
    
class ExtratorAmazon(ExtratorBase):
    HEADERS = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
               "Accept-Language":"pt-BR,pt;q=0.9","Accept":"text/html,*/*;q=0.8"}

    async def extrair(self, link: LinkInfo, uid: int = None) -> Produto:
        ck = f"amz_{link.url_hash}"
        if hit := cache.get(ck):
            return hit

        # GUARDA O LINK ENCURTADO ORIGINAL (com comissão)
        link_encurtado = link.url_original
        
        # RESOLVE para URL longa APENAS para extrair dados
        url_longa = link_encurtado
        if "amzn.to" in url_longa or "a.co" in url_longa:
            url_longa = await self._resolver_url_encurtada(url_longa)
            logger.info(f"[Amazon] URL longa para extração: {url_longa}")

        # Extrai os dados usando a URL longa
        r = requests.get(url_longa, headers=self.HEADERS, timeout=cfg.REQUEST_TIMEOUT)
        soup = BeautifulSoup(r.text, "html.parser")
        
        # Título
        titulo_tag = soup.find("span", id="productTitle") or soup.find("h1")
        titulo = titulo_tag.get_text(strip=True) if titulo_tag else "Produto Amazon"
        
        # Preço
        preco = ""
        for sel in ["span.a-price-whole", "span.a-price .a-offscreen"]:
            el = soup.select_one(sel)
            if el:
                preco = re.sub(r"[^\d,]", "", el.get_text()).replace(",", ".")
                break
        
        # Imagem
        imagem = ""
        img = soup.find("img", id="landingImage")
        if img:
            imagem = img.get("data-old-hires") or img.get("src", "")
        
        # Avaliação
        avaliacao = ""
        r_tag = soup.find("span", {"class": "a-icon-alt"})
        if r_tag:
            m = re.search(r"(\d+[,.]\d+)", r_tag.text)
            if m:
                avaliacao = m.group(1).replace(",", ".")
        
        # CRIA O PRODUTO com o LINK ENCURTADO ORIGINAL (que já tem a comissão)
        produto = Produto(
            titulo=titulo[:120],
            descricao="",
            preco=preco,
            preco_original="",
            imagem=imagem,
            link=link,  # mantém o link original (encurtado com comissão)
            avaliacao=avaliacao,
            metodo="amazon_scraping",
            link_afiliado=link_encurtado,  # USA O LINK ENCURTADO ORIGINAL
        )
        cache.set(ck, produto, ttl=1800)
        return produto

    async def _resolver_url_encurtada(self, url: str) -> str:
        """Resolve URL encurtada para extrair dados."""
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(url, allow_redirects=True,
                                 timeout=aiohttp.ClientTimeout(total=10)) as r:
                    return str(r.url)
        except Exception:
            return url

class ExtratorMercadoLivre(ExtratorBase):
    """Extrai dados de produtos do Mercado Livre usando requests + regex."""

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate",  # 🔥 REMOVIDO "br" (Brotli)
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Cache-Control": "max-age=0",
    }

    DOMINIOS_CURTOS = ["meli.la", "meli.bz", "ml.com.br"]

    async def extrair(self, link: LinkInfo) -> Produto:
        ck = f"ml_{link.url_hash}"

        try:
            hit = cache.get(ck)
            if hit:
                return hit
        except:
            pass

        url = link.url_original
        logger.info(f"[ML] Extraindo: {url}")

        link_afiliado_original = url

        # Resolve URLs curtas
        if any(d in url for d in self.DOMINIOS_CURTOS):
            url_resolvida = await self._resolver_url(url)
            if url_resolvida and url_resolvida != url:
                url = url_resolvida
                logger.info(f"[ML] URL resolvida: {url}")

        produto = await self._buscar_produto(url, link, link_afiliado_original)
        if produto:
            try:
                cache.set(ck, produto, ttl=3600)
            except:
                pass
            return produto

        raise Exception("Produto ML não encontrado")

    async def _resolver_url(self, url: str) -> str:
        """Resolve URLs encurtadas."""
        try:
            import aiohttp
            async with aiohttp.ClientSession() as s:
                async with s.get(url, allow_redirects=True, timeout=15, headers=self.HEADERS) as r:
                    return str(r.url)
        except Exception as e:
            logger.warning(f"[ML] Erro ao resolver URL: {e}")
            return url

    async def _buscar_produto(self, url: str, link: LinkInfo, link_afiliado: str) -> Optional[Produto]:
        """Busca produto via scraping."""
        import aiohttp
        import re
        from bs4 import BeautifulSoup
        
        try:
            async with aiohttp.ClientSession() as s:
                # 🔥 PRIMEIRO: Tentar via URL normal
                async with s.get(url, headers=self.HEADERS, timeout=20, allow_redirects=True) as r:
                    url_final = str(r.url)
                    html = await r.text()
                
                # 🔥 O MACETE: Se falhar, tentar via URL SOCIAL (MLS)
                url_social = re.sub(r'/ML[B-Z]-', '/MLS-', url_final)
                
                logger.info(f"[ML] Tentando URL Social: {url_social}")
                
                async with s.get(url_social, headers=self.HEADERS, timeout=20) as r_social:
                    if r_social.status == 200:
                        html = await r_social.text()
                        url_final = url_social
                        logger.info(f"[ML] ✅ URL Social funcionou!")
                
                # Extrair dados do HTML
                soup = BeautifulSoup(html, 'html.parser')
                
                # Título
                titulo = ''
                titulo_elem = soup.find('h1', class_='ui-pdp-title')
                if not titulo_elem:
                    titulo_elem = soup.find('meta', {'itemprop': 'name'})
                    titulo = titulo_elem.get('content', '') if titulo_elem else ''
                else:
                    titulo = titulo_elem.get_text(strip=True)
                
                if not titulo:
                    meta_title = soup.find('meta', property='og:title')
                    titulo = meta_title.get('content', 'Produto Mercado Livre') if meta_title else 'Produto Mercado Livre'
                
                # ── Preço ────────────────────────────────────────────
                preco = 0.0
                preco_original = 0.0

                def _parse_ml_price(text: str) -> float:
                    """Converte '1.299,90' ou '1299.90' → float."""
                    try:
                        t = text.strip().replace('R$','').strip()
                        # Formato BR: 1.299,90
                        if ',' in t:
                            t = t.replace('.','').replace(',','.')
                        return float(re.sub(r'[^\d.]', '', t))
                    except Exception:
                        return 0.0

                # 1) Preço PROMOCIONAL (segunda linha de preço = preço com desconto)
                promo_block = soup.find('div', class_='ui-pdp-price__second-line')
                if promo_block:
                    frac = promo_block.find('span', class_='andes-money-amount__fraction')
                    if frac:
                        preco = _parse_ml_price(frac.get_text(strip=True))
                        logger.debug(f"[ML] Preço promo: R${preco}")

                # 2) Preço principal (meta itemprop — mais confiável para preço normal)
                if preco == 0:
                    price_meta = soup.find('meta', {'itemprop': 'price'})
                    if price_meta:
                        try:
                            preco = float(price_meta.get('content', 0) or 0)
                            logger.debug(f"[ML] Preço meta: R${preco}")
                        except Exception:
                            pass

                # 3) Fallback: primeiro span com fraction visível
                if preco == 0:
                    for frac in soup.find_all('span', class_='andes-money-amount__fraction'):
                        val = _parse_ml_price(frac.get_text(strip=True))
                        if val > 0:
                            preco = val
                            logger.debug(f"[ML] Preço fallback span: R${preco}")
                            break

                # 4) Preço original (riscado = antes do desconto)
                for sel in [
                    ('s',    'ui-pdp-price__original-value'),
                    ('span', 'andes-money-amount--previous'),
                    ('del',  'andes-money-amount'),
                ]:
                    orig_elem = soup.find(sel[0], class_=sel[1])
                    if orig_elem:
                        val = _parse_ml_price(orig_elem.get_text(strip=True))
                        if val > 0:
                            preco_original = val
                            break

                # Validação: preço original deve ser maior que preço atual
                if preco_original <= preco:
                    preco_original = 0.0
                
                # Desconto
                desconto_pct = 0
                if preco_original > preco:
                    desconto_pct = round((1 - preco / preco_original) * 100)
                
                # Imagem
                imagem = ''
                img_meta = soup.find('meta', property='og:image')
                if img_meta:
                    imagem = img_meta.get('content', '')
                if not imagem:
                    img_elem = soup.find('img', class_='ui-pdp-image')
                    if img_elem:
                        imagem = img_elem.get('src', '')
                
                # Loja
                loja = ''
                seller_elem = soup.find('span', class_='ui-pdp-seller__label-sold')
                if seller_elem:
                    loja = seller_elem.get_text(strip=True).replace('Vendido por', '').strip()
                if not loja:
                    seller_elem = soup.find('a', class_='ui-pdp-seller__link-trigger')
                    if seller_elem:
                        loja = seller_elem.get_text(strip=True)
                
                # Avaliação
                avaliacao = 0.0
                rating_elem = soup.find('meta', {'itemprop': 'ratingValue'})
                if rating_elem:
                    try:
                        avaliacao = float(rating_elem.get('content', 0))
                    except:
                        pass
                if avaliacao == 0:
                    rating_span = soup.find('span', class_='ui-pdp-review__rating')
                    if rating_span:
                        try:
                            avaliacao = float(rating_span.get_text(strip=True))
                        except:
                            pass
                
                # Vendidos
                vendidos = ''
                sold_elem = soup.find('span', class_='ui-pdp-subtitle')
                if sold_elem:
                    sold_text = sold_elem.get_text(strip=True)
                    match = re.search(r'(\d+)\s+vend', sold_text)
                    if match:
                        vendidos = match.group(1)
                
                logger.info(f"[ML] ✅ Extraído: {titulo[:50]} | R$ {preco:.2f}")
                
                # Montar objeto Produto
                produto = Produto(
                    titulo=titulo,
                    preco=f"{preco:.2f}",
                    preco_original=f"{preco_original:.2f}" if preco_original > 0 else "",
                    desconto_pct=desconto_pct,
                    imagem=imagem,
                    loja=loja,
                    vendidos=vendidos,
                    avaliacao=f"{avaliacao:.1f}" if avaliacao > 0 else "",
                    descricao="",
                    link=LinkInfo(
                        url_original=link_afiliado,
                        url_limpa=url_final,
                        plataforma="mercadolivre",
                        eh_afiliado=False,
                        url_hash=link.url_hash
                    ),
                    metodo=f"mercadolivre_scraping",
                    link_afiliado=link_afiliado
                )
                
                return produto
                
        except Exception as e:
            logger.error(f"[ML] ❌ Erro no scraping: {e}")
            return None

class ExtratorMagalu(ExtratorBase):
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
    }

    async def extrair(self, link: LinkInfo) -> Produto:
        ck = f"magalu_{link.url_hash}"
        if hit := cache.get(ck):
            return hit

        url = link.url_original
        
        # Resolve links de afiliado
        if "onelink.me" in url or "divulgador.magalu.com" in url:
            async with aiohttp.ClientSession() as s:
                async with s.get(url, allow_redirects=True, headers=self.HEADERS, timeout=15) as r:
                    url = str(r.url)
                    logger.info(f"[Magalu] URL resolvida: {url}")
                    
                    # Verifica se foi bloqueado
                    if "acessou nosso site de uma forma diferente" in await r.text():
                        raise Exception("Magalu bloqueou o acesso - página de verificação")
        
        # Delay para não parecer bot
        await asyncio.sleep(2)
        
        async with aiohttp.ClientSession() as s:
            async with s.get(url, headers=self.HEADERS, timeout=15) as r:
                if r.status != 200:
                    raise Exception(f"HTTP {r.status}")
                html = await r.text()
                
                # Verifica se é página de bloqueio
                if "acessou nosso site de uma forma diferente" in html:
                    raise Exception("Magalu bloqueou o acesso")
                
                soup = BeautifulSoup(html, "html.parser")
                
                # Título
                titulo_tag = soup.find("h1") or soup.find("meta", property="og:title")
                titulo = "Produto Magalu"
                if titulo_tag:
                    if titulo_tag.get("content"):
                        titulo = titulo_tag["content"]
                    else:
                        titulo = titulo_tag.get_text(strip=True)
                
                # Preço
                preco = ""
                price_selectors = [
                    "[data-testid='price-value']", 
                    ".price-template__price", 
                    ".andes-money-amount__fraction",
                    ".final-price",
                    "[itemprop='price']"
                ]
                for sel in price_selectors:
                    el = soup.select_one(sel)
                    if el:
                        preco_text = el.get_text(strip=True)
                        preco_match = re.search(r"([\d]{1,3}(?:\.\d{3})*,\d{2})", preco_text)
                        if preco_match:
                            preco = preco_match.group(1).replace(".", "").replace(",", ".")
                            break
                
                # Imagem
                imagem = ""
                img_tag = soup.find("meta", property="og:image")
                if img_tag and img_tag.get("content"):
                    imagem = img_tag["content"]
                
                if not titulo or titulo == "Produto Magalu" or not preco:
                    logger.warning(f"[Magalu] Extração incompleta - título: {titulo}, preco: {preco}")
                
                link_info = LinkInfo(
                    url_original=url,
                    url_limpa=url,
                    url_hash=link.url_hash,
                    plataforma="magalu",
                )
                
                produto = Produto(
                    titulo=titulo[:120],
                    descricao="",
                    preco=preco,
                    preco_original="",
                    imagem=imagem,
                    link=link_info,
                    metodo="magalu_scraping",
                    link_afiliado=url,
                )
                cache.set(ck, produto, ttl=1800)
                return produto

    # ------------------------
    # RESOLVER URL
    # ------------------------
    async def _resolver_url(self, url: str) -> str:
        try:
            async with self.session.get(url, headers=self.headers, allow_redirects=True) as resp:
                final = str(resp.url)

            # resolve meli.la
            if "meli.la" in final:
                async with self.session.get(url, headers=self.headers) as resp2:
                    final = str(resp2.url)

            # resolve social via API interna
            if "/social/" in final:
                logger.info("[ML] resolvendo social via API...")
                real = await self._extrair_link_social(final)
                if real:
                    return real

            return final

        except Exception as e:
            logger.warning(f"[ML] erro resolver_url: {e}")
            return url

    # ------------------------
    # SOCIAL → API DO PERFIL
    # ------------------------
    async def _extrair_link_social(self, url: str) -> Optional[str]:
        try:
            match = re.search(r'/social/([a-zA-Z0-9]+)', url)
            if not match:
                return None

            codigo = match.group(1)

            api_url = f"https://www.mercadolivre.com.br/social/api/v1/profile/{codigo}/items"

            async with self.session.get(api_url, headers=self.headers) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()

            items = data.get("items", [])
            if not items:
                return None

            item_id = items[0].get("id") or items[0].get("item_id")

            if not item_id:
                return None

            return f"https://api.mercadolibre.com/items/{item_id}"

        except Exception as e:
            logger.warning(f"[ML] erro social API: {e}")
            return None

    # ------------------------
    # JSON INTERNO
    # ------------------------
    async def _buscar_json_embutido(self, url: str, link: LinkInfo) -> Optional[Produto]:
        async with self.session.get(url, headers=self.headers) as resp:
            html = await resp.text()

        match = re.search(r'__PRELOADED_STATE__\s*=\s*({.*?});', html)
        if not match:
            match = re.search(r'__INITIAL_STATE__\s*=\s*({.*?});', html)

        if not match:
            return None

        data = json.loads(match.group(1))

        item = data.get("initialState", {}).get("components", {}).get("track", {}).get("data", {})
        if not item:
            return None

        titulo = item.get("title", "")
        preco = item.get("price", 0)
        imagem = item.get("pictures", [{}])[0].get("url", "")

        if not preco or not titulo:
            return None

        return Produto(
            titulo=titulo[:120],
            descricao="",
            preco=f"{float(preco):.2f}",
            preco_original="",
            imagem=imagem,
            link=link,
            metodo="json_embutido",
            link_afiliado=url,
            video=""
        )

    # ------------------------
    # API
    # ------------------------
    async def _buscar_via_api(self, url: str, link: LinkInfo) -> Optional[Produto]:
        item_id = self._extrair_item_id(url)
        if not item_id:
            return None

        api_url = f"https://api.mercadolibre.com/items/{item_id}"

        async with self.session.get(api_url, headers=self.headers) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()

        preco = float(data.get('price', 0))
        if preco <= 0:
            return None

        return Produto(
            titulo=data.get('title', '')[:120],
            descricao="",
            preco=f"{preco:.2f}",
            preco_original="",
            imagem=(data.get('pictures') or [{}])[0].get('url', ''),
            link=link,
            metodo="api",
            link_afiliado=data.get('permalink', url),
            video=""
        )

    # ------------------------
    # EXTRAIR ID (CORRIGIDO)
    # ------------------------
    def _extrair_item_id(self, url: str) -> Optional[str]:
        match = re.search(r'[?&]wid=(MLB\d+)', url)
        if match:
            return match.group(1)

        match = re.search(r'[?&]item_id=(MLB\d+)', url)
        if match:
            return match.group(1)

        patterns = [
            r'(MLB\d{8,12})',
            r'/p/(MLB\d+)',
            r'/item/(MLB\d+)',
            r'-(MLB\d+)',
        ]

        for pat in patterns:
            match = re.search(pat, url, re.IGNORECASE)
            if match:
                return match.group(1).upper()

        return None

    # ------------------------
    # SCRAPING
    # ------------------------
    async def _buscar_via_scraping(self, url: str, link: LinkInfo) -> Optional[Produto]:
        async with self.session.get(url, headers=self.headers) as resp:
            html = await resp.text()

        return self._extrair_dados_html(html, url, link, "scraping")

    def _extrair_dados_html(self, html: str, url: str, link: LinkInfo, metodo: str) -> Optional[Produto]:
        if len(html) < 2000:
            return None

        titulo = ""
        preco = 0.0
        imagem = ""

        json_matches = re.findall(r'<script\s+type="application/ld\+json">(.*?)</script>', html, re.DOTALL)
        for json_str in json_matches:
            try:
                data = json.loads(json_str.strip())
                if isinstance(data, dict) and data.get('@type') == 'Product':
                    titulo = data.get('name', titulo)
                    imagem = data.get('image', imagem)
                    if data.get('offers', {}).get('price'):
                        preco = float(data['offers']['price'])
            except:
                pass

        if not titulo:
            m = re.search(r'<meta property="og:title" content="([^"]+)"', html)
            if m:
                titulo = m.group(1)

        if not imagem:
            m = re.search(r'<meta property="og:image" content="([^"]+)"', html)
            if m:
                imagem = m.group(1)

        if preco <= 0:
            m = re.search(r'andes-money-amount__fraction">([\d.,]+)<', html)
            if m:
                try:
                    preco = float(m.group(1).replace('.', '').replace(',', '.'))
                except:
                    pass

        if preco <= 0 or not titulo:
            return None

        return Produto(
            titulo=titulo[:120],
            descricao="",
            preco=f"{preco:.2f}",
            preco_original="",
            imagem=imagem,
            link=link,
            metodo=f"mercadolivre_{metodo}",
            link_afiliado=url,
            video=""
        )
    
class ExtratorGenerico(ExtratorBase):
        async def extrair(self, link: LinkInfo) -> Produto:
            try:
                r    = requests.get(link.url_limpa, headers={"User-Agent":"Mozilla/5.0"}, timeout=10)
                soup = BeautifulSoup(r.text, "html.parser")
                titulo = ""
                for sel in [{"property":"og:title"},{"name":"twitter:title"}]:
                    t = soup.find("meta", sel)
                    if t: titulo = t.get("content",""); break
                if not titulo:
                    t = soup.find("title")
                    titulo = t.get_text() if t else "Oferta Especial"
                imagem = ""
                img = soup.find("meta", property="og:image")
                if img: imagem = img.get("content","")
                precos = re.findall(r"R\$\s*(\d{1,3}(?:\.\d{3})*,\d{2})", r.text)
                preco  = precos[0] if precos else ""
                return Produto(titulo=titulo[:120], descricao="", preco=preco, preco_original="",
                               imagem=imagem, link=link, metodo="generico")
            except Exception:
                return Produto(titulo="Oferta Imperdível 🔥", descricao="", preco="",
                               preco_original="", imagem="", link=link, metodo="fallback")
    
class Extratores:
    _oficial = ExtratorShopeeOficialGraphQL()
    _api = ExtratorShopeeAPI()
    _amazon = ExtratorAmazon()
    _ml = None  # Será inicializado sob demanda
    _magalu = ExtratorMagalu()
    _generico = ExtratorGenerico()
    
    @classmethod
    async def extrair(cls, link: LinkInfo) -> Produto:
        if link.plataforma == "shopee":
            for ext in [cls._oficial, cls._api]:
                try:
                    return await ext.extrair(link)
                except Exception as e:
                    logger.warning(f"[Shopee] {ext.__class__.__name__}: {e}")
            return Produto(titulo="Produto Shopee", descricao="", preco="",
                           preco_original="", imagem="", link=link, metodo="fallback")
        elif link.plataforma == "amazon":
            try:
                return await cls._amazon.extrair(link)
            except Exception:
                return Produto(titulo="Produto Amazon", descricao="", preco="",
                               preco_original="", imagem="", link=link, metodo="fallback")
        elif link.plataforma == "mercadolivre":
            if cls._ml is None:
                cls._ml = ExtratorMercadoLivre()
            try:
                return await cls._ml.extrair(link)
            except Exception as e:
                logger.warning(f"[ML] Extrator falhou: {e}")
                return Produto(titulo="Produto Mercado Livre", descricao="", preco="",
                               preco_original="", imagem="", link=link, metodo="fallback")
        elif link.plataforma == "magalu":  # ← ADICIONE ESTE ELIF
            try:
                return await cls._magalu.extrair(link)
            except Exception as e:
                logger.warning(f"[Magalu] Extrator falhou: {e}")
                return Produto(titulo="Produto Magalu", descricao="", preco="",
                               preco_original="", imagem="", link=link, metodo="fallback")
        else:
            return await cls._generico.extrair(link)
    
    # ══════════════════════════════════════════════════════════════
    #  GERADOR DE COPY
    # ══════════════════════════════════════════════════════════════
ESTILOS = {
        "padrao":      {"nome":"🔥 Padrão",      "templates":["🔥 *{titulo}*\n{desc}⚡ Oferta por tempo limitado!","💥 *{titulo}*\n{desc}🛒 Corre antes que acabe!","🚨 OFERTA RELÂMPAGO!\n*{titulo}*\n{desc}⏱️ Válido enquanto durar!"]},
        "urgencia":    {"nome":"⏰ Urgência",     "templates":["⏰ ÚLTIMAS UNIDADES!\n*{titulo}*\n{desc}🚨 Preço não vai durar!","‼️ NÃO PERCA!\n*{titulo}*\n{desc}⏳ Estoque acabando!"]},
        "engracado":   {"nome":"😂 Divertido",   "templates":["😱 Minha carteira chorou mas meu coração tá feliz!\n*{titulo}*\n{desc}Compra DOIS!😂","🤑 Tá barato demais!\n*{titulo}*\n{desc}Quem não comprar vai se arrepender!🙈"]},
        "tecnico":     {"nome":"⚙️ Técnico",     "templates":["⚙️ *{titulo}*\n{desc}📊 Custo-benefício: EXCELENTE!\n✅ Melhor preço atual.","🔬 *{titulo}*\n{desc}📋 Avaliações confirmam a qualidade.\n💡 Fortemente recomendado!"]},
        "sedutor":     {"nome":"💝 Sedutor",     "templates":["✨ Você merece o melhor!\n*{titulo}*\n{desc}💫 Um presente pra você mesmo(a)...","💝 O luxo que cabe no bolso!\n*{titulo}*\n{desc}Você vale muito mais! ✨"]},
        "informativo": {"nome":"📋 Informativo", "templates":["📦 *{titulo}*\n{desc}Confira e garanta o seu!","🛍️ Produto em destaque:\n*{titulo}*\n{desc}Qualidade com o melhor preço!"]},
    }
    
def _bloco_descricao(produto: Produto) -> str:
    linhas = []
    if produto.preco:
        if produto.preco_original and 5 <= produto.desconto_pct <= 80:
            l = f"~~R$ {produto.preco_original}~~ *R$ {produto.preco}* 🏷️ -{produto.desconto_pct}% OFF"
        else:
            l = f"💰 *R$ {produto.preco}*"
        linhas.append(l)
    if produto.avaliacao and produto.avaliacao not in ("0.0", "0"):
        l = f"⭐ {produto.avaliacao}/5"
        if produto.num_avaliacoes and produto.num_avaliacoes not in ("0", ""):
            l += f" ({produto.num_avaliacoes} avaliações)"
        linhas.append(l)
    if produto.vendidos and produto.vendidos not in ("0", ""):
        linhas.append(f"📦 +{produto.vendidos} vendidos")
    if produto.loja:
        linhas.append(f"🏪 {produto.loja}")
    return ("\n".join(linhas) + "\n\n") if linhas else ""
    
def gerar_copy(produto: Produto, estilo: str = "padrao",
                   template_custom: str = None, uid: int = None) -> str:
        # Se uid fornecido, verifica se tem template ativo
    if uid and template_custom is None:
        idx = db.get_template_ativo(uid)
        if idx >= 0:
            templates = db.get_templates_custom(uid)
            if 0 <= idx < len(templates):
                template_custom = templates[idx].get("template", "")
    # Se produto sem título real, usa fallback mínimo
    if not produto.titulo or produto.titulo in ("Produto Shopee","Produto Amazon","Oferta Especial","Oferta Imperdível 🔥"):
        return (f"🔥 <b>Oferta Imperdível!</b>\n\n"
                f"{'💰 R$ ' + produto.preco if produto.preco else ''}\n"
                f"{'🏷️ -' + str(produto.desconto_pct) + '% OFF' if produto.desconto_pct else ''}\n\n"
                f"⚡ Corre antes que acabe!")
    
    # Preparar variáveis para templates
    preco_original = produto.preco_original or ""
    preco_original_riscado = ""
    if preco_original:
        preco_original_riscado = f"~~R$ {preco_original}~~"
    
    if template_custom:
        try:
            return template_custom.format(
                titulo=produto.titulo[:70],
                desc=_bloco_descricao(produto),
                preco=produto.preco or "",
                loja=produto.loja or "",
                desconto=f"{produto.desconto_pct}%" if produto.desconto_pct else "",
                preco_original=preco_original,
                preco_original_riscado=preco_original_riscado,
            ).strip()
        except Exception:
            pass
    cfg_estilo = ESTILOS.get(estilo, ESTILOS["padrao"])
    template   = random.choice(cfg_estilo["templates"])
    return template.format(titulo=produto.titulo[:70], desc=_bloco_descricao(produto)).strip()
    
    
    # ══════════════════════════════════════════════════════════════
    #  FORMATADORES
    # ══════════════════════════════════════════════════════════════
    
def montar_link_wa(produto: "Produto", uid: int) -> str:
    """
    Gera o link para WhatsApp usando a mesma lógica unificada.
    Para qualquer plataforma, delega para aplicar_afiliado.
    """
    return aplicar_afiliado(uid, produto.link.plataforma,
                            produto.link.url_original, produto.link_afiliado)
    
    
def formatar_mensagem_telegram(uid: int, produto: Produto, copy: str) -> str:
    html_copy = re.sub(r"\*(.+?)\*", r"<b>\1</b>", copy)
    link_final = aplicar_afiliado(uid, produto.link.plataforma,
                                  produto.link.url_original, produto.link_afiliado)
    return f"{html_copy}\n\n🔗 <a href='{link_final}'><b>👉 COMPRAR AGORA</b></a>"
    
    
def formatar_mensagem_whatsapp(uid: int, produto: Produto, copy: str) -> str:
    link_wa = aplicar_afiliado(uid, produto.link.plataforma,
                               produto.link.url_original, produto.link_afiliado)
    return "\n".join([copy, "", "👉 *COMPRAR AGORA:*", link_wa])
    
    # ══════════════════════════════════════════════════════════════
    #  BRIDGE WHATSAPP
    # ══════════════════════════════════════════════════════════════
_wa_status_cache: Dict = {}  # cache {uid: (status, timestamp)}
    
async def wa_status(uid: int) -> Dict:
    import time
    # Cache de 30s para não fazer request a cada clique
    cached = _wa_status_cache.get(uid)
    if cached and time.time() - cached[1] < 30:
        return cached[0]
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{cfg.WA_BRIDGE_URL}/status/{uid}",
                             timeout=aiohttp.ClientTimeout(total=2)) as r:
                data = await r.json()
                _wa_status_cache[uid] = (data, time.time())
                return data
    except Exception:
        return {"connected": False, "hasSession": False}
    
async def wa_bridge_online(bridge_url: str = None) -> bool:
    """
    Verifica se a bridge WhatsApp está online.
    Tenta /status, /health e / como fallback.
    """
    url = (bridge_url or cfg.WA_BRIDGE_URL or "http://localhost:3000").rstrip("/")
    for endpoint in ["/status", "/health", "/"]:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(
                    f"{url}{endpoint}",
                    timeout=aiohttp.ClientTimeout(total=3)
                ) as r:
                    if r.status == 200:
                        try:
                            data = await r.json(content_type=None)
                            # Aceita qualquer resposta com status online/ok/true
                            st = str(data.get("status","")).lower()
                            if st in ("online","ok","ready","connected","true"):
                                return True
                            # Se não tem campo status, apenas estar respondendo é suficiente
                            if endpoint in ("/health", "/"):
                                return True
                        except Exception:
                            # Resposta não-JSON mas status 200 = bridge está de pé
                            return True
        except Exception:
            continue
    return False
    
async def wa_solicitar_codigo(uid: int, telefone: str) -> Dict:
    try:
        payload = {"userId": str(uid), "phoneNumber": telefone.strip().replace(" ", "")}
        url = f"{cfg.WA_BRIDGE_URL}/pairing-code"
        
        # Log para debug no terminal do bot
        logger.info(f"[WA] Chamando bridge: {url}")
        logger.info(f"[WA] Payload: {payload}")
        
        # Usar timeout maior e tentar com sessão
        timeout = aiohttp.ClientTimeout(total=60, connect=10)
        
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, json=payload) as response:
                # Log do status HTTP
                logger.info(f"[WA] Status HTTP: {response.status}")
                
                if response.status != 200:
                    text = await response.text()
                    logger.error(f"[WA] Resposta erro: {text[:200]}")
                    return {"success": False, "error": f"HTTP {response.status}: {text[:100]}"}
                
                data = await response.json()
                logger.info(f"[WA] Resposta: {data}")
                return data
                
    except asyncio.TimeoutError:
        logger.error(f"[WA] Timeout ao chamar bridge")
        return {"success": False, "error": "Timeout: Bridge não respondeu"}
    except aiohttp.ClientConnectorError as e:
        logger.error(f"[WA] Conexão recusada: {e}")
        return {"success": False, "error": "Bridge não está rodando"}
    except Exception as e:
        logger.error(f"[WA] Erro: {type(e).__name__}: {e}")
        return {"success": False, "error": str(e)}
    
async def wa_logout(uid: int) -> bool:
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(f"{cfg.WA_BRIDGE_URL}/logout/{uid}",
                              timeout=aiohttp.ClientTimeout(total=15)) as r:
                data = await r.json()
                return data.get("success", False)
    except Exception:
        return False
    
async def wa_listar_grupos(uid: int) -> List[Dict]:
    """
    Retorna apenas GRUPOS do WhatsApp (filtra conversas individuais e broadcasts).
    Grupos têm ID terminando em @g.us
    Individuais terminam em @s.whatsapp.net ou @c.us
    Broadcasts terminam em @broadcast
    """
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{cfg.WA_BRIDGE_URL}/grupos/{uid}",
                             timeout=aiohttp.ClientTimeout(total=20)) as r:
                if r.status != 200: return []
                data = await r.json()
                # A bridge pode retornar em "grupos", "chats" ou "data"
                grupos_raw = (data.get("grupos")
                              or data.get("chats")
                              or data.get("data")
                              or [])
                result = []
                for g in grupos_raw:
                    if not isinstance(g, dict):
                        continue
                    gid = (g.get("id") or g.get("jid") or g.get("chatId") or "")
                    if not gid:
                        continue
    
                    # ── Filtro principal: só grupos ────────────────────
                    # Grupos WA sempre terminam em @g.us
                    eh_grupo = (
                        "@g.us" in str(gid) or
                        g.get("isGroup") is True or
                        g.get("is_group") is True or
                        g.get("type") in ("group", "grupo") or
                        # participantes > 1 também indica grupo
                        (g.get("participantes") or g.get("participants") or
                         g.get("size") or 0) > 1
                    )
                    # Excluir explicitamente individuais e broadcasts
                    eh_individual = (
                        "@s.whatsapp.net" in str(gid) or
                        "@c.us" in str(gid) or
                        "@broadcast" in str(gid) or
                        g.get("isGroup") is False
                    )
                    if eh_individual:
                        continue
                    if not eh_grupo:
                        continue
    
                    nome = (g.get("nome") or g.get("name") or
                            g.get("subject") or g.get("pushName") or
                            "Grupo sem nome")
                    participantes = (g.get("participantes") or
                                     g.get("participants") or
                                     g.get("size") or 0)
                    # Ignora grupos com nome vazio (chats individuais sem nome)
                    if not nome or nome == "Grupo sem nome" and not "@g.us" in str(gid):
                        continue
    
                    result.append({
                        "id":            str(gid),
                        "nome":          str(nome)[:60],
                        "participantes": participantes,
                    })
    
                logger.info(f"[WA] uid={uid}: {len(grupos_raw)} chats → {len(result)} grupos")
                return result
    except Exception as e:
        logger.warning(f"[WA] wa_listar_grupos uid={uid}: {e}")
        return []
    
async def postar_whatsapp(uid: int, produto: Produto, copy: str,
                              grupos: List[str], bridge_url: str = None) -> Tuple[bool, str, int]:
    if not grupos:
        return False, "Nenhum grupo configurado", 0
    wa_texto = formatar_mensagem_whatsapp(uid, produto, copy)
    enviados = 0
    erros    = []
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=35)) as session:
        for grupo_id in grupos:
            # Garante JID correto:
            # Grupos sempre terminam em @g.us — nunca modificar se já tiver @
            # Se não tiver @, adiciona @g.us (IDs de grupo têm formato 123456789-123@g.us)
            if "@" not in str(grupo_id):
                jid = f"{grupo_id}@g.us"
            else:
                jid = grupo_id  # já está correto, não modificar
            payload = {
                "userId":   str(uid),
                "numero":   jid,
                "mensagem": wa_texto,
                "imagem":   produto.imagem or "",
                "video":    produto.video or "",
            }
            # Tenta apenas 1x — forbidden não precisa de retry
            try:
                async with session.post(f"{bridge_url or cfg.WA_BRIDGE_URL}/send",
                                        json=payload,
                                        timeout=aiohttp.ClientTimeout(total=8)) as resp:
                    data = await resp.json(content_type=None)
                    if resp.status == 200 and data.get("success"):
                        enviados += 1
                    else:
                        err = data.get("error", f"HTTP {resp.status}")
                        erros.append(f"{grupo_id}: {err[:50]}")
            except Exception as e:
                erros.append(f"{grupo_id}: {str(e)[:50]}")
    return enviados > 0, "\n".join(erros), enviados
    
    
    # ══════════════════════════════════════════════════════════════
    # ══════════════════════════════════════════════════════════════
    #  POSTADOR TELEGRAM com retry inteligente
    # ══════════════════════════════════════════════════════════════

# Erros que NÃO devem ser retentados — falha permanente
_ERROS_FATAIS = (
    "Forbidden",
    "kicked",
    "blocked",
    "chat not found",
    "CHANNEL_PRIVATE",
    "bot was blocked",
    "not a member",
    "have no rights",
    "need administrator",
    "user is deactivated",
    "bot is not a member",
)

# Erros que indicam que o bot foi REMOVIDO do canal → remove da lista
_ERROS_REMOVER_CANAL = (
    "kicked from the channel",
    "kicked from the supergroup",
    "Forbidden: bot was kicked",
    "bot is not a member",
    "have no rights to send",
    "not enough rights",
)

class Postador:
    def __init__(self, bot):
        self.bot = bot

    def _e_erro_fatal(self, err: str) -> bool:
        el = err.lower()
        return any(e.lower() in el for e in _ERROS_FATAIS)

    def _deve_remover_canal(self, err: str) -> bool:
        el = err.lower()
        return any(e.lower() in el for e in _ERROS_REMOVER_CANAL)

    async def postar(self, uid: int, produto: Produto,
                     canal: str, copy: str = None) -> Tuple[bool, str]:
        link_final = aplicar_afiliado(uid, produto.link.plataforma,
                                      produto.link.url_original, produto.link_afiliado)
        copy_final = copy if copy else gerar_copy(produto)
        texto  = formatar_mensagem_telegram(uid, produto, copy_final)
        markup = InlineKeyboardMarkup([[
            InlineKeyboardButton("🛒 COMPRAR AGORA", url=link_final)
        ]])

        for tentativa in range(cfg.MAX_RETRY):
            try:
                if produto.video and produto.video.startswith("http"):
                    try:
                        await self.bot.send_video(
                            chat_id=canal,
                            video=produto.video,
                            caption=texto,
                            parse_mode="HTML",
                            reply_markup=markup,
                            thumbnail=produto.imagem or None,
                            supports_streaming=True,
                        )
                    except Exception:
                        if produto.imagem:
                            await self.bot.send_photo(chat_id=canal, photo=produto.imagem,
                                                      caption=texto, parse_mode="HTML",
                                                      reply_markup=markup)
                        else:
                            await self.bot.send_message(chat_id=canal, text=texto,
                                                        parse_mode="HTML", reply_markup=markup)
                elif produto.imagem:
                    await self.bot.send_photo(chat_id=canal, photo=produto.imagem,
                                              caption=texto, parse_mode="HTML",
                                              reply_markup=markup)
                else:
                    await self.bot.send_message(chat_id=canal, text=texto,
                                                parse_mode="HTML", reply_markup=markup)
                return True, ""

            except Exception as e:
                err_str = str(e)

                # Erro fatal → não retenta, retorna imediatamente
                if self._e_erro_fatal(err_str):
                    logger.warning(f"[Postador] ⚠️ Erro fatal em {canal} (sem retry): {err_str[:80]}")
                    # Se o bot foi expulso, remove o canal da lista do usuário
                    if self._deve_remover_canal(err_str):
                        try:
                            a = db.get_assinante(uid)
                            if a:
                                canais = [c for c in a.get("canais_tg", []) if c != canal]
                                ativos = [c for c in a.get("canais_tg_ativos", []) if c != canal]
                                db.set_canais_tg(uid, canais, ativos)
                                logger.warning(
                                    f"[Postador] 🗑️ Canal {canal} REMOVIDO da lista de uid={uid} "
                                    f"(bot foi expulso)")
                                # Notifica o usuário
                                if telegram_app and _main_loop and _main_loop.is_running():
                                    asyncio.run_coroutine_threadsafe(
                                        telegram_app.bot.send_message(
                                            uid,
                                            f"⚠️ <b>Canal removido automaticamente</b>\n\n"
                                            f"O bot foi expulso do canal:\n"
                                            f"<code>{canal}</code>\n\n"
                                            f"O canal foi removido da sua lista. "
                                            f"Adicione novamente após garantir que o bot é admin.",
                                            parse_mode="HTML"),
                                        _main_loop)
                        except Exception as ex:
                            logger.error(f"[Postador] Erro ao remover canal {canal}: {ex}")
                    return False, err_str

                # Erro temporário → retry com backoff
                if tentativa < cfg.MAX_RETRY - 1:
                    wait = cfg.RETRY_DELAY * (tentativa + 1)
                    logger.info(f"[Postador] Retry {tentativa+1}/{cfg.MAX_RETRY} em {canal} após {wait}s")
                    await asyncio.sleep(wait)
                else:
                    logger.error(f"[Postador] ❌ {canal} falhou após {cfg.MAX_RETRY} tentativas: {err_str[:80]}")
                    return False, err_str

        return False, "Erro desconhecido"
    
    
    # ══════════════════════════════════════════════════════════════
    #  AUTO POSTER
    # ══════════════════════════════════════════════════════════════
    
    # Palavras-chave por nicho para filtrar produtos
_PALAVRAS_NICHO = {
    
        # ── ELETRÔNICOS ───────────────────────────────────────────
        "eletronicos": [
            # Celulares e acessórios
            "celular","smartphone","iphone","samsung galaxy","xiaomi","motorola","redmi",
            "poco ","realme","asus zenfone","lg ","sony xperia","oneplus","honor ",
            "capa de celular","película","capinha","carregador turbo","cabo tipo c",
            "cabo lightning","carregador sem fio","powerbank","power bank",
            "fone de ouvido","earphone","earbuds","fone bluetooth","airpods",
            # Computadores e periféricos
            "notebook","laptop","pc gamer","desktop","computador","processador",
            "memória ram","ssd ","hd externo","pen drive","placa mãe","fonte atx",
            "gabinete pc","cooler pc","pasta térmica","teclado mecânico","mouse sem fio",
            "mousepad","monitor ","webcam","hub usb","leitor de cartão",
            # TV e imagem
            "smart tv","televisor","tv 4k","tv 55","tv 65","projetor","chromecast",
            "cabo hdmi","antena digital","conversor digital","receptor digital",
            # Áudio e foto
            "câmera fotográfica","câmera de segurança","câmera ip","câmera wifi",
            "caixa de som","caixa bluetooth","subwoofer","amplificador","soundbar",
            "fone headset","microfone","ring light","estabilizador","tripé",
            # Rede e escritório
            "roteador","wifi","repetidor wifi","switch de rede","impressora",
            "toner","cartucho","scanner","nobreak","estabilizador de tensão",
            # Eletrônicos gerais
            "air fryer","fritadeira elétrica","panela elétrica","multicooker",
            "processador","batedeira elétrica","mixer elétrico","sanduicheira",
            "multilaser","positivo","intelbras","d-link","tp-link",
            "tablet ","ipad ","kindle ","e-reader","smartwatch","relógio inteligente",
            "pulseira inteligente","rastreador gps","drone ","câmera de ação",
        ],
    
        # ── MODA ──────────────────────────────────────────────────
        "moda": [
            # MODA GERAL — palavras sem gênero definido
            "óculos de sol","cinto ","fivela","roupa plus size","roupa infantil",
            "moda praia","roupa fitness","streetwear","moda evangélica","roupa gótica",
            "tênis ","bota ","sandália","chinelo","sapato","mocassim","loafer","ankle boot",
            "bolsa de couro","mochila ","carteira ","relógio ","chapéu","boné ","gorro",
            "conjunto fitness","roupa academia","agasalho",
        ],

        # ── MODA FEMININA ─────────────────────────────────────────
        "moda_feminina": [
            # Roupas femininas
            "vestido","vestido midi","vestido longo","vestido curto","vestido floral",
            "blusa feminina","blusa cropped","cropped ","body feminino","top feminino",
            "saia midi","saia longa","saia plissada","calça feminina","calça skinny",
            "calça wide leg","legging","bermuda feminina","short feminino","conjunto feminino",
            "macacão","jardineira","kimono","camisola","pijama feminino","lingerie",
            "sutiã","calcinha","cinta modeladora","meia calça",
            # Calçados femininos
            "tênis feminino","sandália","salto alto","scarpin","bota feminina",
            "sapatilha","tamanco","rasteirinha","sapato feminino",
            # Acessórios femininos
            "bolsa feminina","bolsa de mão","mochila feminina","carteira feminina",
            "relógio feminino","brinco","colar ","pulseira ","anel ","bracelete",
            "tiara","scrunchie","presilha",
            # Moda feminina geral
            "biquíni","maiô ","roupa feminina","moda feminina","roupas femininas",
        ],

        # ── MODA MASCULINA ────────────────────────────────────────
        "moda_masculina": [
            # Roupas masculinas
            "camiseta masculina","camisa masculina","polo masculina","regata masculina",
            "calça jeans","calça masculina","bermuda masculina","short masculino",
            "moletom masculino","conjunto masculino","pijama masculino",
            "cueca ","meia masculina","roupa masculina","roupas masculinas",
            # Calçados masculinos
            "tênis masculino","bota masculina","sapato masculino",
            # Acessórios masculinos
            "carteira masculina","relógio masculino",
            # Moda masculina geral
            "sunga ","moda masculina","roupas de homem",
        ],
        "casa": [
            # Sala
            "sofá ","sofa ","poltrona","puff ","rack para tv","mesa de centro",
            "mesa lateral","tapete sala","tapete peludo","tapete persa","cortina ",
            "persiana","lustre","pendente","abajur","luminária de pé","espelho",
            "quadro decorativo","almofada","capa de almofada","manta sofá",
            # Quarto
            "cama box","colchão","travesseiro","protetor de colchão","lençol",
            "jogo de cama","edredom","cobertor","manta ","guarda-roupa","armário",
            "criado-mudo","penteadeira","cabideiro","organizador de roupa",
            # Cozinha
            "panela ","conjunto de panelas","frigideira","wok ","caçarola",
            "panela de pressão","air fryer","fritadeira","micro-ondas","liquidificador",
            "batedeira","mixer ","processador de alimentos","torradeira",
            "cafeteira nespresso","chaleira elétrica","garrafa térmica",
            "pote ","tigela ","vasilha",
            "escorredor","fruteira","lixeira cozinha","utensílios de cozinha",
            "tábua de corte","faca de cozinha","conjunto de facas","afiador",
            "kit cozinha","jogo de panelas","forma de bolo","assadeira",
            "kit de cozinha","jogo cozinha","silicone cozinha","utensílio",
            "spatula","espátula","concha ","escumadeira","ralador",
            # Banheiro
            "toalha de banho","toalha de rosto","jogo de toalhas","tapete de banheiro",
            "suporte para toalha","porta shampoo","saboneteira","dispenser",
            "box de banheiro","espelho de banheiro","acessórios banheiro",
            # Decoração e organização
            "vaso ","enfeite ","escultura","estatueta","relógio de parede",
            "porta-retrato","organizador","caixa organizadora","cabide ","prateleira",
            "estante ","nicho ","cômoda ","gaveteiro","porta-joias","porta-tempero",
            # Limpeza e utilidades
            "vassoura","rodo ","balde ","esfregão","mop ","aspirador","robô aspirador",
            "ferro de passar","tábua de passar","secador ","ventilador","ar condicionado",
            "purificador","umidificador","desumidificador",
        ],
    
        # ── BELEZA GERAL ──────────────────────────────────────────
        "beleza": [
            # Unissex / neutros
            "shampoo ","condicionador ","máscara de cabelo","creme de pentear",
            "óleo capilar","leave-in","finalizador","tintura de cabelo","descolorante",
            "matizador","tonalizante","chapinha","prancha de cabelo","babyliss",
            "secador de cabelo","modelador de cabelo","difusor","escova rotativa",
            "desodorante","sabonete","hidratante corporal","óleo corporal",
            "esfoliante corporal","creme para mãos","pomada cicatrizante","talco ",
            "protetor solar","boticário","o boticário","natura ","avon ","l'oreal",
            "pantene","seda ","dove ","nívea","salon line","elseve",
        ],

        # ── BELEZA FEMININA ───────────────────────────────────────
        "beleza_feminina": [
            # Skincare feminina
            "hidratante facial","sérum facial","vitamina c facial","ácido hialurônico",
            "retinol","limpeza facial","esfoliante facial","máscara facial","tônico facial",
            "água micelar","primer ","base facial","corretivo ","contorno ","iluminador facial",
            "skincare","cuidado com a pele","antiidade","anti-rugas","clareador",
            "manchas na pele","acne",
            # Maquiagem
            "batom ","lip gloss","lápis labial","blush ","bronzer ","pó compacto",
            "pó facial","paleta de sombra","sombra ","delineador","máscara de cílios",
            "cílios postiços","sobrancelha","lápis de sobrancelha","fixador de maquiagem",
            "kit de pincéis","pincel maquiagem","esponja beauty blender",
            "base de maquiagem","bb cream","cc cream","corretivo de olheira",
            # Perfumes femininos
            "perfume feminino","colônia feminina",
            # Unhas e depilação feminina
            "esmalte ","kit de unhas","alicate","lixa de unhas","gel uv","gel led",
            "unhas de gel","unhas acrílicas","cera depilatória","depilador feminino",
            # Corpo feminino
            "loção corporal feminina","creme redutor","creme modelador","celulite",
            # Marcas femininas
            "maybelline","floratta","veet ","gillette venus",
        ],

        # ── BELEZA MASCULINA ──────────────────────────────────────
        "beleza_masculina": [
            # Barba e barbear
            "barbeador","gillette","aparelho de barbear","gel de barbear","navalha",
            "barba ","produto para barba","creme de barbear","pós-barba","loção pós-barba",
            "balm barba","óleo de barba","pomada de barba","pente barba","aparador barba",
            "kit barba","barbearia",
            # Skincare masculino
            "hidratante masculino","sérum masculino","skincare masculino",
            "protetor solar masculino","limpeza pele masculina","cuidado pele homem",
            # Perfumes masculinos
            "perfume masculino","colônia masculina","eau de toilette masculino",
            # Cabelo masculino
            "pomada capilar","gel capilar","cera capilar","wax cabelo","leave-in masculino",
            "shampoo masculino","condicionador masculino","antiqueda masculino",
            # Depilação masculina
            "depilador masculino","lâmina masculina",
            # Marcas masculinas
            "rexona men","nivea men","dove men","old spice","axe ","brut ",
        ],
    
        # ── ESPORTES ──────────────────────────────────────────────
        "esportes": [
            # Futebol e bola
            "bola de futebol","bola de basquete","bola de vôlei","bola de tênis",
            "chuteira society","chuteira campo","chuteira futsal","uniforme de futebol",
            "caneleira","meião de futebol","luva de goleiro","rede de gol",
            # Academia e musculação
            "haltere ","anilha ","kettlebell","barra musculação","supino","rack academia",
            "esteira ergométrica","bike ergométrica","bicicleta spinning",
            "elíptico","remo ergométrico","corda de pular","colchonete","tapete de yoga",
            "yoga ","pilates","faixa elástica","extensor","theraband","foam roller",
            # Corrida e ciclismo
            "tênis de corrida","tênis esportivo","meia esportiva","bermuda de corrida",
            "camiseta dry fit","legging esportiva","shorts esportivo","regata esportiva",
            "bicicleta ","bike mtb","bike speed","capacete ciclismo","luva ciclismo",
            # Lutas e artes marciais
            "kimono ","judô","karatê","jiu-jitsu","boxe","luva de boxe","protetor bucal",
            "caneleira muay thai","saco de pancada","bandagem",
            # Natação e aventura
            "óculos de natação","touca de natação","prancha de natação",
            "mochila de trilha","barraca de camping","saco de dormir","lanterna",
            "boné esportivo","viseira ","garrafinha esporte","coqueteleira",
            # Suplementos esportivos
            "barra de proteína","suplemento esportivo","pre-treino","pré treino",
        ],
    
        # ── GAMES ─────────────────────────────────────────────────
        "games": [
            # Consoles e controles
            "playstation","ps5 ","ps4 ","ps3 ","xbox series","xbox one","nintendo switch",
            "nintendo 3ds","game boy","controle ps5","controle ps4","controle xbox",
            "joystick","controle sem fio","controle bluetooth","console de video game",
            "videogame ","video game",
            # PC Gamer
            " gamer"," gaming","pc gamer","mouse gamer","teclado gamer","headset gamer",
            "cadeira gamer","monitor gamer","placa de vídeo","gpu gamer","processador intel",
            "processador amd","ryzen ","core i","memória ddr","fonte gamer","gabinete gamer",
            "cooler gamer","water cooler","rgb gamer","fone gamer","suporte headset",
            # Jogos e mídias
            "jogo ps4","jogo ps5","jogo xbox","jogo nintendo","jogo pc","game ps",
            "mídia física","código de jogo","gift card steam","gift card psn",
            # Acessórios gamer
            "mousepad gamer","base de carregamento","dock para switch","carregador controle",
            "grip controle","capinha controle","adaptador controle","conversor de controle",
            # Cultura geek
            "funko pop","action figure","boneco colecionável","miniatura ","estatueta gamer",
            "mangá ","hq ","quadrinho ","anime ","figura de anime","cosplay",
            "poster gamer","caneca gamer","luminária gamer","relógio gamer",
        ],
    
        # ── AUTOMOTIVO ────────────────────────────────────────────
        "automotivo": [
            # Peças e manutenção
            "para carro","automotivo","pneu ","câmara de ar","roda ","calota ",
            "pastilha de freio","disco de freio","filtro de ar","filtro de óleo",
            "vela de ignição","correia dentada","amortecedor","rolamento ",
            "lâmpada automotiva","lâmpada led automotiva","farol ","lanterna traseira",
            # Som e acessórios internos
            "som automotivo","rádio automotivo","dvd automotivo","central multimídia",
            "subwoofer automotivo","módulo amplificador","alto-falante carro",
            "câmera de ré","sensor de estacionamento","retrovisor camera",
            "suporte celular carro","suporte veicular","carregador veicular",
            "cabo auxiliar","bluetooth automotivo","adaptador carro",
            # Proteção e limpeza
            "tapete automotivo","capa de banco","capa de volante","capa de câmbio",
            "película automotiva","tintamento","insulfilm","protetor solar automotivo",
            "cera automotiva","shampoo automotivo","limpa vidros","limpa estofado",
            "aspirador automotivo","compressor de ar","manômetro",
            # Segurança
            "câmera dashcam","câmera frontal carro","rastreador veicular","gps automotivo",
            "alarme automotivo","trava de volante","extintor de incêndio",
            # Externos e acessórios
            "rack de teto","portamalas","estepe","triângulo","macaco hidráulico",
            "chave de roda","cabo de bateria","carregador de bateria automotivo",
        ],
    
        # ── BEBÊS E INFANTIL ──────────────────────────────────────
        "bebes": [
            # Bebê 0-2 anos
            "bebê ","para bebê","fralda ","fralda descartável","lenço umedecido",
            "mamadeira ","chupeta ","mordedor ","chocalho ","móbile berço",
            "berço ","mini berço","bercinho","carrinho de bebê","moisés",
            "bebê conforto","cadeirinha de carro","cadeira de bebê","cadeirão",
            "banheira de bebê","trocador","lençol berço","kit berço","enxoval bebê",
            "roupa de bebê","body bebê","macacão bebê","saída maternidade",
            "amamentação","absorvente mamário","bomba de leite","cinta pós parto",
            # Criança 2-10 anos
            "brinquedo infantil","boneca barbie","boneco ","pelúcia ","ursinho",
            "carrinho de brinquedo","lego ","blocos de montar","quebra-cabeça",
            "jogo de tabuleiro","dominó","baralho","pipa ","bolinha de sabão",
            "massinha de modelar","kit de pintura","canetinha","giz de cera",
            "mochila infantil","estojo infantil","lápis de cor","caderno infantil",
            "andador ","triciclo","bicicleta infantil","patinete infantil",
            "piscina infantil","escorregador","balanço infantil",
            # Alimentação infantil
            "papinha","comida de bebê","leite em pó","vitamina infantil",
            "prato infantil","colher de silicone","copo com alça",
            # Saúde infantil
            "termômetro infantil","nebulizador","porta chupeta","esterilizador",
            "baby monitor","monitor de bebê",
        ],
    
        # ── ALIMENTOS E BEBIDAS ───────────────────────────────────
        "alimentos": [
            # Bebidas
            "café ","café em pó","café solúvel","cápsula de café","nespresso",
            "chá ","suco ","refrigerante","água com gás","energético",
            "cerveja ","vinho ","whisky ","vodka ","gin ","cachaça",
            # Alimentos básicos
            "arroz ","feijão ","macarrão","massa ","farinha ","açúcar ","sal ",
            "azeite ","óleo de cozinha","molho ","tempero ","condimento ",
            # Snacks e doces
            "chocolate ","bombom ","trufa ","brigadeiro","biscoito ","cookie ",
            "bolo ","paçoca ","cocada ","amendoim ","castanha ","nozes ",
            "pipoca ","chips ","salgadinho","barra de cereal","granola ",
            # Saudável e orgânico
            "orgânico","integral ","sem glúten","vegano ","sem lactose",
            "proteína ","whey protein","creatina ","bcaa ","colágeno ",
            "vitamina c","vitamina d","ômega 3","probiótico","multivitamínico",
            "pasta de amendoim","pasta de castanha","proteína vegetal",
            "aveia ","quinoa ","chia ","linhaça ","cúrcuma ","spirulina",
            # Suplementos
            "suplemento alimentar","hipercalórico","malto ","dextrose ",
            "termogênico","l-carnitina","glutamina ","albumina ",
            # Gourmet e especiais
            "mel ","geleia ","doce ","sorvete ","frozen","iogurte ","queijo ",
            "presunto ","salame ","embutido ","defumado ","bacalhau ",
        ],
    
        "mercadolivre": [
            "mercadolivre", "mercado livre", "ml", "full", "frete grátis",
            "entrega rápida", "produto novo", "liquidação",
        ],
    }
    
def _produto_bate_nicho(produto: "Produto", nicho) -> bool:
    """
    Verifica se produto pertence ao nicho (str) ou a qualquer nicho da lista.
    Lista vazia / "todos" = aceita qualquer produto.

    Busca em: título + descrição + loja + categoria do produto.
    Requer match REAL — não faz fallback.
    """
    if not nicho or nicho == "todos":
        return True
    nichos_lista: List[str] = nicho if isinstance(nicho, list) else [nicho]
    if not nichos_lista or "todos" in nichos_lista:
        return True

    # Monta texto de busca com todos os campos relevantes do produto
    titulo     = (produto.titulo or "").lower()
    descricao  = (produto.descricao or "").lower()
    loja       = (produto.loja or "").lower()
    categoria  = (produto.categoria or "").lower()
    # Texto completo para busca — espaços nas bordas garantem match de palavra inteira
    texto = f" {titulo} {descricao} {loja} {categoria} "

    # Coleta todas as palavras-chave dos nichos solicitados
    palavras_combinadas: List[str] = []
    for n in nichos_lista:
        palavras_n = _PALAVRAS_NICHO.get(n, [])
        if not palavras_n:
            # Nicho sem lista de palavras = usa o nome do nicho como palavra-chave
            palavras_combinadas.append(n.replace("_", " "))
        else:
            palavras_combinadas.extend(palavras_n)

    for p in palavras_combinadas:
        # Busca a palavra-chave no texto (com espaço antes para evitar match parcial)
        if f" {p}" in texto or p in texto:
            return True

    return False
    
class AutoPoster:
    def __init__(self, bot):
        self.bot       = bot
        self.ativo     = True
        self._postador = Postador(bot)

    async def loop(self):
        logger.info(f"[AutoPoster] Iniciado – intervalo {cfg.AUTO_POSTER_INTERVALO}min")
        await asyncio.sleep(10)
        while self.ativo:
            try:
                await self._ciclo()
            except Exception as e:
                logger.error(f"[AutoPoster] Erro no ciclo: {e}")
                if "ConnectError" in str(e) or "NetworkError" in str(e) or "No address" in str(e):
                    logger.info("[AutoPoster] Erro de rede — aguardando 60s e tentando de novo")
                    await asyncio.sleep(60)
                    continue
            await asyncio.sleep(cfg.AUTO_POSTER_INTERVALO * 60)

    async def _ciclo(self):
        if not cfg.SHOPEE_APP_ID or not cfg.SHOPEE_SECRET:
            logger.warning("[AutoPoster] Credenciais Shopee não configuradas — ciclo ignorado")
            return

        # Cria client fresco por ciclo (sessão aiohttp no loop correto)
        client = ShopeeAffiliateGraphQL(cfg.SHOPEE_APP_ID, cfg.SHOPEE_SECRET)

        try:
            assinantes = [a for a in db.listar_assinantes() if a.get("modo_auto") == 1]
            if not assinantes:
                logger.info("[AutoPoster] Nenhum usuário com auto-poster ativo")
                return

            # ── Verificação Neon de cada usuário ──────────────────────
            validos = []
            for assinante in assinantes:
                uid = assinante["id"]
                try:
                    async with aiohttp.ClientSession() as s:
                        async with s.get(
                            f"https://botautomacao.vercel.app/verificar/{uid}",
                            timeout=aiohttp.ClientTimeout(total=5)
                        ) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                if not data.get("ativo"):
                                    db._exec(
                                        "UPDATE assinantes SET ativo=0, modo_auto=0 WHERE id=%s", (uid,))
                                    logger.info(f"[AutoPoster] uid={uid} desativado (Neon)")
                                    continue
                except Exception as e:
                    logger.warning(f"[AutoPoster] Neon check uid={uid}: {e} — mantendo")
                validos.append(assinante)

            if not validos:
                return

            # ── Busca produtos ────────────────────────────────────────
            TODOS_NICHOS = list(CATEGORIAS_AUTO.keys())
            produtos_por_nicho: Dict[str, List[Produto]] = {}
            try:
                todos_produtos = await client.buscar_melhores_promocoes(limite=50, categoria="todos")
                logger.info(f"[AutoPoster] Shopee: {len(todos_produtos)} produtos")

                if cfg.ML_ACCESS_TOKEN or (cfg.ML_CLIENT_ID and cfg.ML_SECRET_KEY):
                    try:
                        ml_produtos = await MercadoLivreAPI.buscar_promocoes(limite=30)
                        todos_produtos.extend(ml_produtos)
                        logger.info(f"[AutoPoster] ML: {len(ml_produtos)} adicionados")
                    except Exception as e:
                        logger.warning(f"[AutoPoster] ML falhou: {e}")

                produtos_por_nicho["todos"] = todos_produtos
                for nicho in TODOS_NICHOS:
                    if nicho == "todos":
                        continue
                    produtos_por_nicho[nicho] = [
                        p for p in todos_produtos if _produto_bate_nicho(p, nicho)
                    ]
                logger.info(f"[AutoPoster] {len(todos_produtos)} produtos | {len(validos)} usuários")

            except Exception as e:
                logger.error(f"[AutoPoster] Busca falhou: {e}")
                return

            # ── Processa cada usuário ─────────────────────────────────
            for assinante in validos:
                uid = assinante["id"]
                try:
                    if not db.pode_auto_postar(uid):
                        logger.info(f"[AutoPoster] uid={uid} ainda no intervalo — pulando")
                        continue
                    if not db.assinatura_ativa(uid):
                        continue

                    _t0 = time.time()
                    min_desc     = assinante.get("min_desconto", 20)
                    estilo       = assinante.get("estilo", "padrao")
                    bridge_url   = assinante.get("wa_bridge_url") or cfg.WA_BRIDGE_URL
                    canais_tg    = [c for c in assinante.get("canais_tg", [])
                                    if c in assinante.get("canais_tg_ativos", [])]
                    grupos_wa    = [g for g in assinante.get("grupos_wa", [])
                                    if g in assinante.get("grupos_wa_ativos", [])]
                    nichos_tg    = db.get_nichos_tg(uid)
                    nichos_wa    = db.get_nichos_wa(uid)
                    templates_tg = db.get_templates_tg(uid)
                    templates_wa = db.get_templates_wa(uid)
                    all_templates= db.get_templates_custom(uid)

                    logger.info(f"[AutoPoster] ▶️ uid={uid} | {len(canais_tg)} TG | {len(grupos_wa)} WA")

                    if not canais_tg and not grupos_wa:
                        continue

                    db.limpar_auto_postagens_antigas(uid, dias=15)

                    # ── Telegram ──────────────────────────────────────
                    for canal in canais_tg:
                        if time.time() - _t0 > 180:
                            logger.warning(f"[AutoPoster] uid={uid} timeout TG 180s")
                            break

                        nichos_canal: List[str] = nichos_tg.get(canal, [])
                        if nichos_canal:
                            # Canal tem nicho(s) configurado(s) — filtra ESTRITAMENTE
                            pool = [p for p in todos_produtos if _produto_bate_nicho(p, nichos_canal)]
                            if not pool:
                                # Sem produtos do nicho neste ciclo — tenta busca direta
                                logger.info(
                                    f"[AutoPoster] uid={uid} canal={canal} nichos={nichos_canal} "
                                    f"sem produtos no pool — buscando diretamente")
                                try:
                                    for nicho_k in nichos_canal:
                                        extras = await client.buscar_por_nicho_direto(nicho_k, 20)
                                        pool.extend(
                                            p for p in extras
                                            if _produto_bate_nicho(p, nichos_canal)
                                        )
                                except Exception as e:
                                    logger.warning(f"[AutoPoster] busca direta nicho: {e}")
                                if not pool:
                                    # Ainda sem produtos — pula este canal SEM fallback para todos
                                    logger.info(
                                        f"[AutoPoster] uid={uid} canal={canal} "
                                        f"sem produtos do nicho {nichos_canal} — PULANDO (não faz fallback)")
                                    continue
                        else:
                            # Canal sem nicho configurado = aceita todos
                            pool = produtos_por_nicho.get("todos", [])

                        postou_neste_canal = False
                        for produto in pool:
                            hash_key = produto.link.url_hash + canal
                            if db.ja_auto_postou(uid, hash_key):         continue
                            if db.em_blacklist(uid, produto):             continue
                            if produto.desconto_pct < min_desc:           continue
                            if not produto.preco or not produto.titulo:   continue
                            if not produto.imagem:                        continue

                            tmpl_idx = templates_tg.get(canal, -1)
                            if tmpl_idx >= 0 and 0 <= tmpl_idx < len(all_templates):
                                copy = gerar_copy(produto, estilo,
                                                  all_templates[tmpl_idx].get("template", ""))
                            else:
                                copy = gerar_copy(produto, estilo, uid=uid)

                            try:
                                if _main_loop is None or not _main_loop.is_running():
                                    logger.error("[AutoPoster] _main_loop não disponível")
                                    break
                                future = asyncio.run_coroutine_threadsafe(
                                    self._postador.postar(uid, produto, canal, copy),
                                    _main_loop
                                )
                                ok, erro = future.result(timeout=35)
                                if ok:
                                    db.inc_postagem(uid, "telegram")
                                    db.log_postagem(
                                        uid, produto.link.url_hash, canal, True,
                                        metodo=produto.metodo, destino="telegram",
                                        url=produto.link.url_original,
                                        titulo=produto.titulo, imagem=produto.imagem,
                                        preco=produto.preco, desconto=produto.desconto_pct)
                                    db.registrar_auto_postagem(
                                        uid, hash_key,
                                        produto.titulo, produto.preco, produto.desconto_pct)
                                    logger.info(
                                        f"[AutoPoster] ✅ TG {canal} | "
                                        f"{produto.titulo[:35]} | -{produto.desconto_pct}%")
                                    postou_neste_canal = True
                                    break  # Postou neste canal — vai para o próximo
                                else:
                                    # Erro fatal (Forbidden/kicked) → o Postador já removeu o canal
                                    # Apenas loga e passa para o próximo canal
                                    if any(e.lower() in erro.lower() for e in _ERROS_FATAIS):
                                        logger.warning(f"[AutoPoster] ⛔ Canal {canal} com erro fatal — pulando para próximo")
                                        break  # Sai do loop de produtos, vai para próximo canal
                                    logger.warning(f"[AutoPoster] TG falhou: {erro[:80]}")
                            except Exception as e:
                                logger.warning(f"[AutoPoster] TG uid={uid} canal={canal}: {str(e)[:80]}")
                                # Se erro fatal, pula para próximo canal
                                if any(e_fatal.lower() in str(e).lower() for e_fatal in _ERROS_FATAIS):
                                    break

                        if not postou_neste_canal:
                            logger.debug(f"[AutoPoster] Canal {canal} — nenhum produto adequado")

                    # ── WhatsApp ──────────────────────────────────────
                    if grupos_wa:
                        try:
                            bridge_ok = await wa_bridge_online(bridge_url)
                        except Exception:
                            bridge_ok = False

                        for grupo in grupos_wa:
                            if time.time() - _t0 > 180:
                                logger.warning(f"[AutoPoster] uid={uid} timeout WA 180s")
                                break

                            # nichos_wa[grupo] agora é List[str] ou ausente (= todos)
                            nichos_grupo: List[str] = nichos_wa.get(grupo, [])
                            if nichos_grupo:
                                # Grupo tem nicho(s) configurado(s) — filtra ESTRITAMENTE
                                pool = [p for p in todos_produtos if _produto_bate_nicho(p, nichos_grupo)]
                                if not pool:
                                    logger.info(
                                        f"[AutoPoster] uid={uid} grupo={grupo[:20]} nichos={nichos_grupo} "
                                        f"sem produtos no pool — buscando diretamente")
                                    try:
                                        for nicho_k in nichos_grupo:
                                            extras = await client.buscar_por_nicho_direto(nicho_k, 20)
                                            pool.extend(
                                                p for p in extras
                                                if _produto_bate_nicho(p, nichos_grupo)
                                            )
                                    except Exception as e:
                                        logger.warning(f"[AutoPoster] busca direta nicho WA: {e}")
                                    if not pool:
                                        logger.info(
                                            f"[AutoPoster] uid={uid} grupo={grupo[:20]} "
                                            f"sem produtos do nicho {nichos_grupo} — PULANDO (não faz fallback)")
                                        continue
                            else:
                                # Grupo sem nicho = aceita todos
                                pool = produtos_por_nicho.get("todos", [])

                            for produto in pool:
                                hash_key = produto.link.url_hash + grupo
                                if db.ja_auto_postou(uid, hash_key):         continue
                                if db.em_blacklist(uid, produto):             continue
                                if produto.desconto_pct < min_desc:           continue
                                if not produto.preco or not produto.titulo:   continue
                                if not produto.imagem:                        continue

                                tmpl_idx = templates_wa.get(grupo, -1)
                                if tmpl_idx >= 0 and 0 <= tmpl_idx < len(all_templates):
                                    copy = gerar_copy(produto, estilo,
                                                      all_templates[tmpl_idx].get("template", ""))
                                else:
                                    copy = gerar_copy(produto, estilo, uid=uid)

                                if not bridge_ok:
                                    logger.warning(f"[AutoPoster] Bridge offline — WA uid={uid} pulado")
                                    break

                                try:
                                    future_wa = asyncio.run_coroutine_threadsafe(
                                        postar_whatsapp(uid, produto, copy, [grupo], bridge_url),
                                        _main_loop
                                    )
                                    ok_wa, _, _ = future_wa.result(timeout=35)
                                    if ok_wa:
                                        db.inc_postagem(uid, "whatsapp")
                                        db.log_postagem(
                                            uid, produto.link.url_hash, grupo, True,
                                            metodo=produto.metodo, destino="whatsapp",
                                            url=produto.link.url_original,
                                            titulo=produto.titulo, imagem=produto.imagem,
                                            preco=produto.preco, desconto=produto.desconto_pct)
                                        db.registrar_auto_postagem(
                                            uid, hash_key,
                                            produto.titulo, produto.preco, produto.desconto_pct)
                                        logger.info(
                                            f"[AutoPoster] ✅ WA {grupo[:20]} | {produto.titulo[:35]}")
                                        await asyncio.sleep(1)
                                        break
                                except Exception as e:
                                    logger.warning(
                                        f"[AutoPoster] WA uid={uid} grupo={grupo[:15]}: {str(e)[:80]}")

                    _tempo_total = round(time.time() - _t0)
                    logger.info(f"[AutoPoster] ✔️ uid={uid} concluído em {_tempo_total}s")
                    db.registrar_auto_post_ts(uid)
                    await asyncio.sleep(1)

                except Exception as e:
                    logger.error(f"[AutoPoster] Erro uid={uid}: {e}")
                    continue

        finally:
            # Fecha sessão aiohttp do client Shopee ao fim do ciclo
            try:
                await client.fechar()
            except Exception:
                pass

    def iniciar(self):
        """Inicia o auto-poster em thread dedicada com loop próprio."""
        def _run():
            # Aguarda _main_loop estar disponível e rodando
            for _ in range(120):
                if _main_loop is not None and _main_loop.is_running():
                    break
                time.sleep(1)
            else:
                logger.error("[AutoPoster] _main_loop não ficou disponível em 120s — abortando")
                return
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(self.loop())
            finally:
                loop.close()
        t = threading.Thread(target=_run, daemon=True, name="AutoPoster")
        t.start()
        logger.info(f"[AutoPoster] Thread iniciada — intervalo {cfg.AUTO_POSTER_INTERVALO}min")
    
    # ══════════════════════════════════════════════════════════════
    #  POSTADOR AGENDADO
    # ══════════════════════════════════════════════════════════════
class PostadorAgendado:
    """
    Agendador de postagens.
    Roda no loop principal do Telegram (sem criar loop separado)
    para evitar deadlock com asyncio.run_coroutine_threadsafe.
    """

    def __init__(self, bot):
        self.bot       = bot
        self._postador = Postador(bot)

    async def loop(self):
        while True:
            try:
                await self._verificar()
            except Exception as e:
                logger.error(f"[Agenda] Erro no loop: {e}", exc_info=True)
            await asyncio.sleep(60)

    async def _verificar(self):
        agora     = datetime.now().strftime("%H:%M")
        pendentes = db.pendentes(agora)
        if not pendentes:
            return

        logger.info(f"[Agenda] {len(pendentes)} agendamento(s) para {agora}")

        for p in pendentes:
            uid  = p.get("user_id")
            aid  = p.get("id")
            if not uid:
                continue
            # Marca como postado ANTES de postar para evitar duplo envio
            # (se der erro, loga mas não reposta)
            db.marcar_postado(aid)

            if not db.assinatura_ativa(uid):
                logger.info(f"[Agenda] uid={uid} sem assinatura ativa — pulando")
                continue

            try:
                await self._processar(uid, p)
            except Exception as e:
                logger.error(f"[Agenda] uid={uid} id={aid}: {e}", exc_info=True)

    async def _processar(self, uid: int, p: dict):
        url      = p.get("url", "")
        url_hash = p.get("url_hash", "")
        destinos = p.get("destinos", "telegram")
        a        = db.get_assinante(uid)
        bridge   = (a or {}).get("wa_bridge_url") or cfg.WA_BRIDGE_URL

        if not url:
            logger.warning(f"[Agenda] uid={uid} sem URL — pulando")
            return

        # ── Extrai produto ────────────────────────────────────────
        try:
            link    = LinkAnalyzer.analisar(url)
            produto = await Extratores.extrair(link)
        except Exception as e:
            logger.error(f"[Agenda] uid={uid} erro ao extrair {url[:60]}: {e}")
            return

        if not produto or not produto.titulo:
            logger.warning(f"[Agenda] uid={uid} produto inválido para {url[:60]}")
            return

        # ── Link com afiliado — usa a URL já salva (que tem o afiliado aplicado)
        # Mas garante que o link final tem o código de afiliado do usuário
        link_final = aplicar_afiliado(
            uid,
            produto.link.plataforma,
            url,  # URL já salva com afiliado pelo processar_horario_prod
            produto.link_afiliado,
        )
        # Injeta o link final no produto para o Postador usar
        produto = produto.__class__(
            **{**produto.__dict__,
               "link_afiliado": link_final}
        ) if hasattr(produto, "__dict__") else produto

        # ── Copy: usa a customizada se existir, senão gera do estilo ──
        copy_custom = db.get_copy_custom(uid, url_hash)
        if copy_custom:
            copy = copy_custom
            logger.debug(f"[Agenda] uid={uid} usando copy customizada")
        else:
            estilo = (a.get("estilo", "padrao") if a else "padrao")
            copy   = gerar_copy(produto, estilo, uid=uid)

        # ── Telegram ──────────────────────────────────────────────
        if "telegram" in destinos and a:
            canais_ativos = [c for c in a.get("canais_tg", [])
                             if c in a.get("canais_tg_ativos", [])]
            canais_post, _, _ = _filtrar_destinos_por_nicho(uid, produto, canais_ativos, [])

            for canal in canais_post:
                try:
                    ok, erro = await self._postador.postar(uid, produto, canal, copy)
                    db.log_postagem(
                        uid, url_hash, canal, ok, erro,
                        produto.metodo, "telegram",
                        url=link_final, titulo=produto.titulo,
                        imagem=produto.imagem, preco=produto.preco,
                        desconto=produto.desconto_pct)
                    if ok:
                        db.inc_postagem(uid, "telegram")
                        logger.info(f"[Agenda] ✅ TG uid={uid} canal={canal}")
                    else:
                        logger.warning(f"[Agenda] ❌ TG uid={uid} canal={canal}: {erro[:80]}")
                except Exception as e:
                    logger.error(f"[Agenda] TG uid={uid} canal={canal}: {e}")

        # ── WhatsApp ──────────────────────────────────────────────
        if "whatsapp" in destinos and a:
            grupos_ativos = [g for g in a.get("grupos_wa", [])
                             if g in a.get("grupos_wa_ativos", [])]
            _, grupos_post, _ = _filtrar_destinos_por_nicho(uid, produto, [], grupos_ativos)

            if grupos_post:
                if await wa_bridge_online(bridge):
                    try:
                        ok_wa, _, enviados = await postar_whatsapp(
                            uid, produto, copy, grupos_post, bridge)
                        if ok_wa:
                            db.inc_postagem(uid, "whatsapp")
                            logger.info(f"[Agenda] ✅ WA uid={uid} {enviados}/{len(grupos_post)}")
                        else:
                            logger.warning(f"[Agenda] ❌ WA uid={uid} falhou")
                    except Exception as e:
                        logger.error(f"[Agenda] WA uid={uid}: {e}")
                else:
                    logger.warning(f"[Agenda] WA bridge offline — uid={uid}")

    def iniciar(self):
        """
        NÃO cria loop separado — agenda a coroutine no loop principal do Telegram.
        Isso evita o deadlock com asyncio.run_coroutine_threadsafe.
        """
        async def _agendar():
            # Aguarda o loop principal estar pronto
            await asyncio.sleep(5)
            logger.info("[Agenda] Agendador iniciado no loop principal")
            await self.loop()

        if _main_loop and _main_loop.is_running():
            asyncio.run_coroutine_threadsafe(_agendar(), _main_loop)
        else:
            # Fallback: thread própria (compatível com versões antigas)
            def _run():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                loop.run_until_complete(self.loop())
            threading.Thread(target=_run, daemon=True, name="Agenda").start()
        logger.info("[Agenda] Agendador de postagens iniciado")
    
    
    # ══════════════════════════════════════════════════════════════
    #  VERIFICADOR DE ASSINATURAS EXPIRADAS
    # ══════════════════════════════════════════════════════════════
class VerificadorAssinaturas:
    """
    Roda a cada hora e:
    1. Desativa assinantes cujo vencimento passou (NeonDB).
    2. Para modo_auto dos expirados.
    3. Notifica o usuário que a assinatura expirou.
    """
    def __init__(self, bot):
        self.bot = bot

    async def loop(self):
        while True:
            try:
                await self._verificar()
            except Exception as e:
                logger.error(f"[VerifAssin] {e}")
            await asyncio.sleep(3600)  # 1h

    async def _verificar(self):
        """
        Verifica expiração de forma segura:
        - Busca todos os assinantes ativos
        - Parseia o vencimento em Python (lida com YYYY-MM, DD/MM/YYYY, etc.)
        - Só desativa se vencimento parseado < hoje (sem ambiguidade de formato)
        - Margem de 1 dia para evitar falsos positivos de fuso horário
        """
        hoje = datetime.now().date()
        margem = timedelta(days=1)  # só desativa se expirou há mais de 1 dia

        todos = db._exec("""
            SELECT id, nome, vencimento FROM assinantes WHERE ativo=1
        """, fetch="all") or []

        for row in todos:
            uid       = row["id"]
            nome      = row.get("nome") or str(uid)
            venc_raw  = row.get("vencimento") or ""
            venc_str  = Database._parse_date(venc_raw)

            if not venc_str:
                # Vencimento não parseável — não desativa (segurança)
                logger.warning(f"[VerifAssin] uid={uid} vencimento inválido '{venc_raw}' — ignorado")
                continue

            try:
                venc_date = datetime.strptime(venc_str, "%Y-%m-%d").date()
            except ValueError:
                logger.warning(f"[VerifAssin] uid={uid} data inválida '{venc_str}' — ignorado")
                continue

            # Só desativa se expirou há MAIS de 1 dia (evita fuso/formato errado)
            if venc_date < (hoje - margem):
                db.desativar(uid)
                logger.info(f"[VerifAssin] uid={uid} ({nome}) desativado — venceu em {venc_str}")
                try:
                    await self.bot.send_message(
                        uid,
                        "⏰ <b>Sua assinatura expirou!</b>\n\n"
                        "O acesso ao bot foi suspenso.\n\n"
                        "Use /start para renovar e continuar postando! 🚀",
                        parse_mode="HTML",
                        reply_markup=InlineKeyboardMarkup([[
                            InlineKeyboardButton("💳 Renovar Agora",
                                                 url=criar_link_pagamento(uid))
                        ]]))
                except Exception as e:
                    logger.warning(f"[VerifAssin] Não foi possível notificar uid={uid}: {e}")

    def iniciar(self):
        def _run():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self.loop())
        threading.Thread(target=_run, daemon=True, name="VerifAssin").start()
        logger.info("[VerifAssin] Iniciado — verificação a cada 1h")


    # ══════════════════════════════════════════════════════════════
    #  MONITOR DE VENCIMENTOS + RELATÓRIO SEMANAL
    # ══════════════════════════════════════════════════════════════
class MonitorVencimentos:
        def __init__(self, bot):
            self.bot = bot
    
        async def loop(self):
            while True:
                try:
                    await self._checar_vencimentos()
                    await self._enviar_relatorios()
                except Exception as e:
                    logger.error(f"[Monitor] {e}")
                await asyncio.sleep(3600)  # a cada 1h
    
        async def _checar_vencimentos(self):
            vencendo = db.assinantes_vencendo(dias=3)
            for a in vencendo:
                uid  = a["id"]
                venc = Database._parse_date(a["vencimento"])
                dias_rest = (datetime.strptime(venc,"%Y-%m-%d") - datetime.now()).days
                try:
                    plano_info = PLANOS.get(a.get("plano","mensal"),{})
                    link_mp = criar_link_pagamento(uid)
                    await self.bot.send_message(
                        uid,
                        f"⚠️ <b>Sua assinatura vence em {dias_rest} dia(s)!</b>\n\n"
                        f"📋 Plano: <b>{plano_info.get('nome','Mensal')}</b>\n"
                        f"📅 Vencimento: <code>{venc}</code>\n\n"
                        f"Renove agora para não perder o acesso! 👇",
                        parse_mode="HTML",
                        reply_markup=InlineKeyboardMarkup([[
                            InlineKeyboardButton("💳 Renovar Agora", url=link_mp)
                        ]]))
                except Exception as e:
                    err = str(e).lower()
                    if "forbidden" in err or "blocked" in err:
                        # Usuário bloqueou o bot — desativa para não tentar mais
                        logger.info(f"[Monitor] uid={uid} bloqueou o bot — desativando")
                        db.desativar(uid)
                    pass
    
        async def _enviar_relatorios(self):
            if not PLANOS.get("pro",{}).get("relatorio"):
                return
            assinantes = db.listar_assinantes()
            hoje = datetime.now()
            if hoje.weekday() != 0:  # Só segunda-feira
                return
            for a in assinantes:
                uid = a["id"]
                if not db.plano_permite(uid, "relatorio"):
                    continue
                ultimo = a.get("ultimo_relatorio","")
                if ultimo:
                    try:
                        diff = (hoje - datetime.fromisoformat(ultimo)).days
                        if diff < 6: continue
                    except Exception:
                        pass
                try:
                    stats   = db.stats_historico_semana(uid)
                    por_dia = db.stats_historico_por_dia(uid, 7)
                    grafico = ""
                    for d in por_dia:
                        barras = "█" * min(d["total"], 10)
                        dia_fmt = d["dia"][5:]  # MM-DD
                        grafico += f"  {dia_fmt}: {barras} {d['total']}\n"
                    if not grafico:
                        grafico = "  Nenhuma postagem esta semana.\n"
                    await self.bot.send_message(
                        uid,
                        f"📊 <b>Relatório Semanal</b>\n\n"
                        f"📬 Total: <b>{stats['total']}</b> postagens\n"
                        f"📢 Telegram: <b>{stats['tg']}</b>\n"
                        f"📲 WhatsApp: <b>{stats['wa']}</b>\n\n"
                        f"<b>Posts por dia:</b>\n{grafico}\n"
                        f"Bom trabalho! Continue assim 🚀",
                        parse_mode="HTML")
                    db._exec(
                        "UPDATE assinantes SET ultimo_relatorio=%s WHERE id=%s",
                        (hoje.isoformat(), uid))
                except Exception:
                    pass
    
        def iniciar(self):
            def _run():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                loop.run_until_complete(self.loop())
            threading.Thread(target=_run, daemon=True, name="Monitor").start()
    
    
    # ══════════════════════════════════════════════════════════════
    #  HELPERS / UI
    # ══════════════════════════════════════════════════════════════
def is_admin(uid: int) -> bool: return uid == cfg.ID_ADMIN
    
def btn(text: str, cb: str)     -> InlineKeyboardButton: return InlineKeyboardButton(text, callback_data=cb)
def btn_url(text: str, url: str) -> InlineKeyboardButton: return InlineKeyboardButton(text, url=url)
def kb(*rows)                   -> InlineKeyboardMarkup:  return InlineKeyboardMarkup(list(rows))
    
VOLTAR_MAIN = [[btn("🏠 Menu Principal", "main_menu")]]
    
def _nome_curto_grupo(gid: str) -> str:
    parte = gid.split("@")[0]
    return f"Grupo …{parte[-8:]}"
    
async def reply(update: Update, text: str, markup=None, parse="HTML"):
    m = update.effective_message
    if update.callback_query:
        try: await m.edit_text(text, parse_mode=parse, reply_markup=markup); return
        except Exception: pass
    await m.reply_text(text, parse_mode=parse, reply_markup=markup)
    
def ativar_sync(uid, dias=30, plano="mensal", email=None, nome=None, username=None) -> Optional[str]:
    try:
        venc = db.ativar(uid, dias, plano, email, nome, username)
        plano_info = PLANOS.get(plano, {})
        if telegram_app and _main_loop and _main_loop.is_running():
            asyncio.run_coroutine_threadsafe(
                telegram_app.bot.send_message(
                    uid,
                    f"✅ <b>Assinatura ativada!</b>\n\n"
                    f"📋 Plano: <b>{plano_info.get('nome', plano.upper())}</b>\n"
                    f"📅 Válida até: <b>{venc}</b>\n\n"
                    f"Use /start para começar! 🚀",
                    parse_mode="HTML"),
                _main_loop)
        return venc
    except Exception as e:
        logger.error(f"ativar_sync: {e}"); return None
    
    
def teclado_main(uid: int) -> InlineKeyboardMarkup:
    ativo = db.assinatura_ativa(uid)
    if ativo:
        a        = db.get_assinante(uid) or {}
        modo_str = "🤖 AUTO: ON" if a.get("modo_auto") else "👤 AUTO: OFF"
        plano    = a.get("plano","mensal")
        return kb(
            [btn("📢 Canais Telegram",   "menu_canais_tg"),   btn("📲 Grupos WhatsApp",  "menu_grupos_wa")],
            [btn("📤 Postar Produto",    "menu_postar"),       btn("📚 Biblioteca",        "menu_links")],
            [btn("⏰ Agendamentos",      "menu_agenda"),       btn("📊 Estatísticas",      "menu_stats")],
            [btn(modo_str,               "menu_auto"),          btn("🎨 Estilo de Copy",   "menu_estilo")],
            [btn("🌐 Nichos",            "menu_nichos"),        btn("📝 Templates",        "menu_templates")],
            [btn("🔗 Meus Afiliados",   "menu_afiliados"),    btn("⚙️ Configurações",   "menu_config")],
            [btn("🕐 Histórico",        "menu_historico"),    btn("🚫 Blacklist",         "menu_blacklist")],
            [btn("🎁 Indicar Amigos",   "menu_referral"),     btn("❓ Ajuda",             "menu_ajuda")],
            [btn_url("🆘 Suporte", cfg.SUPORTE_LINK)],
        )
    else:
        link_pagamento = criar_link_pagamento(uid)
        rows = []
        if not db.usou_teste(uid):
            rows.append([btn("🎁 TESTAR 7 DIAS GRÁTIS", "teste_gratis")])
        rows.append([btn_url("💳 Assinar por R$19,99/mês", link_pagamento)])
        rows.append([btn_url("🆘 Suporte", cfg.SUPORTE_LINK)])
        return InlineKeyboardMarkup(rows)
    
    
    # ══════════════════════════════════════════════════════════════
    #  ONBOARDING GUIADO
    # ══════════════════════════════════════════════════════════════
ONBOARDING_STEPS = {
        1: {
            "titulo": "Passo 1/4 – Canais Telegram",
            "texto": (
                "🎉 <b>Bem-vindo ao Bot Afiliados PRO!</b>\n\n"
                "Vamos configurar tudo em 4 passos rápidos!\n\n"
                "📢 <b>Passo 1: Adicionar um Canal ou Grupo Telegram</b>\n\n"
                "1️⃣ Adicione o bot como <b>administrador</b> no seu canal/grupo\n"
                "2️⃣ Clique em <b>➕ Adicionar Canal</b> abaixo\n"
                "3️⃣ Selecione o canal na lista do Telegram\n\n"
                "<i>Pode pular e fazer depois se preferir.</i>"
            ),
        },
        2: {
            "titulo": "Passo 2/4 – Link de Afiliado",
            "texto": (
                "🔗 <b>Passo 2: Configure seu Link de Afiliado Shopee</b>\n\n"
                "Seu código de afiliado é inserido <b>automaticamente</b> em todos os posts!\n\n"
                "📌 Como encontrar seu código:\n"
                "1️⃣ Acesse shopee.com.br → Centro de Afiliados\n"
                "2️⃣ Copie seu ID de afiliado\n"
                "3️⃣ Cole abaixo\n\n"
                "<i>Sem código? Pode pular — você ainda posta normalmente.</i>"
            ),
        },
        3: {
            "titulo": "Passo 3/4 – Estilo de Copy",
            "texto": (
                "🎨 <b>Passo 3: Escolha seu Estilo de Copy</b>\n\n"
                "O bot gera o texto dos posts automaticamente!\n"
                "Escolha o estilo que combina com seu público:"
            ),
        },
        4: {
            "titulo": "Passo 4/4 – Modo Automático",
            "texto": (
                "🤖 <b>Passo 4: Ativar o Modo Automático</b>\n\n"
                "O bot busca as melhores promoções da Shopee e posta <b>sozinho</b>!\n\n"
                "⚙️ Configure:\n"
                "• Desconto mínimo desejado\n"
                "• O bot posta a cada {intervalo} minutos\n\n"
                "<i>Você pode ativar/desativar a qualquer momento.</i>"
            ).format(intervalo=cfg.AUTO_POSTER_INTERVALO),
        },
    }
    
async def tela_onboarding(update: Update, ctx: ContextTypes.DEFAULT_TYPE, step: int = None):
    uid  = update.effective_user.id
    if step is None:
        step = db.get_onboarding(uid)
        if step >= 4:
            await cmd_start(update, ctx)
            return
    
    step_info = ONBOARDING_STEPS.get(step + 1, ONBOARDING_STEPS[1])
    progresso = "●" * (step) + "○" * (4 - step)
    
    texto = (f"<b>{step_info['titulo']}</b>  {progresso}\n\n"
             + step_info['texto'])
    
    if step == 0:  # passo 1
        rows = [
            [btn("➕ Adicionar Canal/Grupo", "tg_listar")],
            [btn("⏭️ Pular", "onboard_skip_1")],
        ]
    elif step == 1:  # passo 2
        rows = [
            [btn("🔗 Configurar Shopee", "aff_edit_shopee")],
            [btn("⏭️ Pular", "onboard_skip_2")],
        ]
    elif step == 2:  # passo 3
        rows = [[btn(v["nome"], f"onboard_estilo_{k}")] for k, v in ESTILOS.items()]
        rows.append([btn("⏭️ Pular", "onboard_skip_3")])
    elif step == 3:  # passo 4
        rows = [
            [btn("🟢 Ativar Automático", "onboard_auto_on")],
            [btn("⏭️ Pular", "onboard_skip_4")],
        ]
    else:
        rows = [[btn("🏠 Ir para o Menu", "main_menu")]]
    
    await reply(update, texto, InlineKeyboardMarkup(rows), "HTML")
    
    
    # ══════════════════════════════════════════════════════════════
    #  TELAS PRINCIPAIS
    # ══════════════════════════════════════════════════════════════
async def tela_ver_planos(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    link_mp = criar_link_pagamento(uid)
    texto = (
        "💳 <b>Assinatura – R$19,99/mês</b>\n\n"
        "✅ Acesso completo a <b>todos</b> os recursos:\n\n"
        "  📢 Canais Telegram ilimitados\n"
        "  📲 Grupos WhatsApp ilimitados\n"
        "  🤖 Auto-poster inteligente (Shopee)\n"
        "  ⏰ Agendamento de postagens\n"
        "  🔗 Links de afiliado automáticos\n"
        "  📝 Templates de copy personalizados\n"
        "  📊 Relatório semanal automático\n"
        "  🚫 Blacklist de produtos/lojas\n"
        "  🌐 Filtro por categoria\n"
        "  🎁 Programa de indicações\n\n"
        "🛡️ Pagamento seguro via <b>Mercado Pago</b>\n"
        "📅 Renovação automática mensal\n\n"
    )
    if not db.usou_teste(uid):
        texto += "🎁 <b>Comece com 7 dias GRÁTIS!</b>"
    rows = []
    if not db.usou_teste(uid):
        rows.append([btn("🎁 7 Dias GRÁTIS", "teste_gratis")])
    rows.append([btn_url("💳 Assinar R$19,99/mês", link_mp)])
    rows += VOLTAR_MAIN
    await reply(update, texto, InlineKeyboardMarkup(rows), "HTML")
    
    
async def tela_stats(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    a    = db.get_assinante(uid)
    stats = db.get_stats(uid)
    semana = db.stats_historico_semana(uid)
    por_dia = db.stats_historico_por_dia(uid, 7)
    
    total_tg  = stats.get("total_postagens", 0)
    total_wa  = stats.get("total_wa", 0)
    ultima    = stats.get("ultima_postagem")
    ultima_str = ultima[:16].replace("T", " ") if ultima else "Nunca"
    
    canais_tg = a.get("canais_tg", []) if a else []
    ativos_tg = a.get("canais_tg_ativos", []) if a else []
    grupos_wa = a.get("grupos_wa", []) if a else []
    ativos_wa = a.get("grupos_wa_ativos", []) if a else []
    
    modo_auto    = bool(a.get("modo_auto", 0)) if a else False
    min_desconto = a.get("min_desconto", 20) if a else 20
    estilo       = ESTILOS.get(a.get("estilo","padrao") if a else "padrao",{}).get("nome","Padrão")
    
    venc     = a.get("vencimento","N/A") if a else "N/A"
    plano    = a.get("plano","mensal") if a else "N/A"
    plano_nm = PLANOS.get(plano,{}).get("nome", plano.upper())
    refs     = db.stats_referral(uid)
    
    # Mini gráfico da semana
    grafico = ""
    if por_dia:
        max_v = max(d["total"] for d in por_dia) or 1
        for d in por_dia:
            barras  = "█" * round(d["total"] / max_v * 8)
            dia_fmt = d["dia"][5:]
            grafico += f"  {dia_fmt}: {barras or '▏'} {d['total']}\n"
    else:
        grafico = "  Sem dados esta semana.\n"
    
    texto = (
        f"📊 <b>Estatísticas</b>\n\n"
        f"📋 Plano: <b>{plano_nm}</b>  |  📅 Vence: <code>{venc}</code>\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📬 <b>Total de Postagens</b>\n"
        f"  📢 Telegram: <b>{total_tg}</b>\n"
        f"  📲 WhatsApp: <b>{total_wa}</b>\n"
        f"  📅 Esta semana: <b>{semana.get('total',0)}</b>\n"
        f"  🕐 Última: <code>{ultima_str}</code>\n\n"
        f"📈 <b>Últimos 7 dias:</b>\n{grafico}\n"
        f"🎯 <b>Destinos</b>\n"
        f"  📢 TG: <b>{len(ativos_tg)}/{len(canais_tg)}</b> ativos\n"
        f"  📲 WA: <b>{len(ativos_wa)}/{len(grupos_wa)}</b> ativos\n\n"
        f"⚙️ <b>Config</b>\n"
        f"  🤖 Auto: <b>{'🟢 ON' if modo_auto else '🔴 OFF'}</b>  "
        f"🏷️ Mín: <b>{min_desconto}%</b>  "
        f"🎨 <b>{estilo}</b>\n\n"
        f"🤝 <b>Indicações:</b> {refs['total']} recompensadas  "
        f"⏳ {refs['pendentes']} pendentes"
    )
    await reply(update, texto, kb(VOLTAR_MAIN[0]), "HTML")
    
    
async def tela_historico(update: Update, ctx: ContextTypes.DEFAULT_TYPE, pagina: int = 0):
    uid  = update.effective_user.id
    hist = db.listar_historico(uid, limite=50)
    if not hist:
        await reply(update,
            "🕐 <b>Histórico de Postagens</b>\n\nNenhuma postagem ainda.",
            kb(VOLTAR_MAIN[0]), "HTML"); return
    
    por_pag = 5
    inicio  = pagina * por_pag
    fim     = inicio + por_pag
    pag_atual = hist[inicio:fim]
    total_pags = (len(hist) - 1) // por_pag
    
    texto = f"🕐 <b>Histórico de Postagens</b> ({len(hist)} total)\n\n"
    rows  = []
    for h in pag_atual:
        emoji  = "📢" if h.get("destino") == "telegram" else "📲"
        data   = (h.get("postado_em") or "")[:16].replace("T"," ")
        titulo = (h.get("titulo") or "Produto")[:30]
        preco  = f" R${h['preco']}" if h.get("preco") else ""
        desc   = f" -{h['desconto']}%" if h.get("desconto") else ""
        texto += f"{emoji} <b>{titulo}</b>{preco}{desc}\n<i>{data}</i>\n\n"
        rows.append([
            btn(f"🔄 Repostar – {titulo[:20]}", f"repost_{h['id']}"),
        ])
    
    # Paginação
    nav = []
    if pagina > 0:
        nav.append(btn("◀️ Anterior", f"hist_pag_{pagina-1}"))
    if pagina < total_pags:
        nav.append(btn("Próxima ▶️", f"hist_pag_{pagina+1}"))
    if nav:
        rows.append(nav)
    rows += VOLTAR_MAIN
    await reply(update, texto, InlineKeyboardMarkup(rows), "HTML")
    
    
async def tela_blacklist(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    a   = db.get_assinante(uid)
    bl_lojas = a.get("blacklist_lojas", []) if a else []
    bl_prods = a.get("blacklist_produtos", []) if a else []
    
    texto = (
        "🚫 <b>Blacklist</b>\n\n"
        "Produtos de lojas ou com palavras na blacklist são\n"
        "<b>ignorados</b> pelo auto-poster.\n\n"
    )
    if bl_lojas:
        texto += "<b>Lojas bloqueadas:</b>\n"
        for l in bl_lojas:
            texto += f"  🔴 {l}\n"
        texto += "\n"
    if bl_prods:
        texto += "<b>Palavras bloqueadas (título):</b>\n"
        for p in bl_prods:
            texto += f"  🔴 {p}\n"
        texto += "\n"
    if not bl_lojas and not bl_prods:
        texto += "<i>Nenhum item na blacklist.</i>"
    
    rows = [
        [btn("➕ Bloquear Loja",    "bl_add_loja"),
         btn("➕ Bloquear Palavra", "bl_add_palavra")],
    ]
    for i, l in enumerate(bl_lojas[:5]):
        rows.append([btn(f"❌ {l[:30]}", f"bl_del_loja_{i}")])
    for i, p in enumerate(bl_prods[:5]):
        rows.append([btn(f"❌ {p[:30]}", f"bl_del_palavra_{i}")])
    rows += VOLTAR_MAIN
    await reply(update, texto, InlineKeyboardMarkup(rows), "HTML")
    
    
async def tela_templates(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    a    = db.get_assinante(uid)
    templates = db.get_templates_custom(uid)
    limite    = db.get_limite_plano(uid, "templates_custom")
    plano     = (a.get("plano","mensal") if a else "mensal")
    tem_acesso = limite > 0
    
    texto = (
        f"📝 <b>Templates Personalizados</b> ({len(templates)}/{limite})\n\n"
        "Crie seus próprios textos de copy!\n\n"
        "<b>Variáveis disponíveis:</b>\n"
        "  <code>{titulo}</code> – Nome do produto\n"
        "  <code>{preco}</code> – Preço atual\n"
        "  <code>{loja}</code> – Nome da loja\n"
        "  <code>{desconto}</code> – % de desconto\n"
        "  <code>{desc}</code> – Bloco completo (preço+avaliação)\n"
        "  <code>{preco_original}</code> – Preço original\n"
        "  <code>{preco_original_riscado}</code> – Preço original já com riscado\n\n"
    )
    
    # plano único — acesso liberado para todos os assinantes
    
    rows = []
    for i, t in enumerate(templates):
        rows.append([
            btn(f"👁️ {t['nome'][:25]}", f"tmpl_ver_{i}"),
            btn("🗑️", f"tmpl_del_{i}"),
        ])
    if len(templates) < limite:
        rows.append([btn("➕ Criar Template", "tmpl_novo")])
    rows += VOLTAR_MAIN
    await reply(update, texto, InlineKeyboardMarkup(rows), "HTML")
    
async def tela_canais_tg(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid      = update.effective_user.id
    print("🔍 DEBUG: Iniciando tela_canais_tg")
    uid      = update.effective_user.id
    print("🔍 DEBUG: uid ok")
    a        = db.get_assinante(uid)
    print("🔍 DEBUG: assinante ok")
    canais   = a.get("canais_tg", []) if a else []
    print(f"🔍 DEBUG: canais = {canais}")
    ativos   = a.get("canais_tg_ativos", []) if a else []
    print(f"🔍 DEBUG: ativos = {ativos}")
    limite   = a.get("limite_canais", 10) if a else 10
    print(f"🔍 DEBUG: limite = {limite}")
    
    canais = [str(c) for c in canais]
    print(f"🔍 DEBUG: canais convertido = {canais}")
    ativos = [str(a) for a in ativos]
    print(f"🔍 DEBUG: ativos convertido = {ativos}")
    
    chats_db = {}
    print("🔍 DEBUG: chats_db criado")
    
    try:
        bot_chats = db.listar_bot_chats()
        print(f"🔍 DEBUG: bot_chats = {bot_chats[:2] if bot_chats else 'vazio'}")
        for item in bot_chats:
            try:
                if hasattr(item, 'keys'):
                    k = str(item.get('chat_id', ''))
                    v = str(item.get('titulo', ''))
                    if k and k != 'None':
                        chats_db[k] = v
                elif hasattr(item, '__iter__') and not isinstance(item, str):
                    if len(item) >= 2:
                        k = str(item[0])
                        v = str(item[1])
                        if k and k != 'None':
                            chats_db[k] = v
            except Exception as e:
                print(f"🔍 DEBUG: erro no item {item}: {e}")
                continue
    except Exception as e:
        print(f"🔍 DEBUG: erro ao carregar bot_chats: {e}")
    
    print(f"🔍 DEBUG: chats_db = {list(chats_db.keys())[:5]}")
    a        = db.get_assinante(uid)
    canais   = a.get("canais_tg", []) if a else []
    ativos   = a.get("canais_tg_ativos", []) if a else []
    limite   = a.get("limite_canais", 10) if a else 10
    
    # Forçar conversão para strings
    canais = [str(c) for c in canais]
    ativos = [str(a) for a in ativos]
    
    # Criar chats_db VAZIO e preencher manualmente
    chats_db = {}
    
    # Tentar carregar do banco de forma SUPER segura
    try:
        bot_chats = db.listar_bot_chats()
        for item in bot_chats:
            try:
                # Tentar como dicionário
                if hasattr(item, 'keys'):
                    k = str(item.get('chat_id', ''))
                    v = str(item.get('titulo', ''))
                    if k and k != 'None':
                        chats_db[k] = v
                # Tentar como lista/tupla
                elif hasattr(item, '__iter__') and not isinstance(item, str):
                    if len(item) >= 2:
                        k = str(item[0])
                        v = str(item[1])
                        if k and k != 'None':
                            chats_db[k] = v
            except:
                continue
    except:
        pass
    
    # Buscar informações de canais não encontrados
    for c in canais:
        if c and c not in chats_db:
            try:
                chat_ref = int(c) if c.lstrip("-").isdigit() else c
                chat_info = await ctx.bot.get_chat(chat_ref)
                titulo = str(chat_info.title or chat_info.username or c)
                db.registrar_bot_chat(c, titulo, chat_info.type)
                chats_db[c] = titulo
            except:
                chats_db[c] = str(c)

    texto = "📢 <b>Canais e Grupos Telegram</b>\n\n"
    if canais:
        texto += "🟢 = ativo   🔴 = pausado  (clique para alternar)\n\n"
        for i, c in enumerate(canais):
            if not c:
                continue
            st = "🟢" if c in ativos else "🔴"
            nome = str(chats_db.get(c, c))
            texto += f"  {i+1}. {st} <b>{nome}</b>\n"
        texto += f"\n<i>{len(canais)}/{limite} destinos</i>"
    else:
        texto += "Nenhum canal/grupo configurado.\nClique em ➕ para adicionar!"

    nichos_tg = db.get_nichos_tg(uid)
    rows = []
    for c in canais:
        if not c:
            continue
        st = "🟢" if c in ativos else "🔴"
        nome = str(chats_db.get(c, c))[:20]
        # 🔥 CORREÇÃO: Garantir que o nicho seja uma string
        nicho_val = nichos_tg.get(c, "todos")
        if isinstance(nicho_val, list):
            nicho_val = nicho_val[0] if nicho_val else "todos"
        nicho = CATEGORIAS_AUTO.get(nicho_val, "🌐 Todos")
        rows.append([
            btn(f"{st} {nome[:32]}", f"tg_toggle_{c}"),
            btn("🗑️", f"tg_confirm_del_{c}"),
        ])
    rows.append([btn("➕ Adicionar canal/grupo", "tg_listar")])
    rows += VOLTAR_MAIN
    await reply(update, texto, InlineKeyboardMarkup(rows), "HTML")
async def tela_grupos_wa(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid    = update.effective_user.id
    a      = db.get_assinante(uid)
    grupos = a.get("grupos_wa", []) if a else []
    ativos = a.get("grupos_wa_ativos", []) if a else []
    nomes_cache = db.get_nomes_grupos_wa(uid)
    
    # Verifica status da bridge — com timeout curto para não travar
    try:
        status_wa  = await wa_status(uid)
        conectado  = status_wa.get("connected", False)
        tem_sessao = status_wa.get("hasSession", False)
    except Exception:
        conectado  = False
        tem_sessao = False
    
    # Se tem grupos salvos, considera conectado mesmo que status falhe
    # (bridge pode estar lenta mas WA funcionando)
    tem_grupos_salvos = len(grupos) > 0
    status_str = "🟢 Conectado" if conectado else ("⚠️ Verificando..." if tem_grupos_salvos else "🔴 Desconectado")
    
    texto = f"📲 <b>WhatsApp</b>  {status_str}\n\n"
    
    # Só bloqueia se não tiver grupos salvos E não estiver conectado
    if not conectado and not tem_grupos_salvos:
        if tem_sessao:
            texto += "Sessão salva mas desconectada.\nClique em <b>Reconectar</b>."
        else:
            texto += "Conecte seu WhatsApp para postar em grupos!\nUse código de pareamento — rápido e seguro 📱"
        rows = [[btn("📱 Conectar WhatsApp", "wa_connect")]]
        rows += VOLTAR_MAIN
        await reply(update, texto, InlineKeyboardMarkup(rows), "HTML")
        return
    
    if grupos:
        texto += "🟢 = ativo   🔴 = pausado\n\n"
        for i, g in enumerate(grupos):
            st   = "🟢" if g in ativos else "🔴"
            nome = nomes_cache.get(g) or _nome_curto_grupo(g)
            texto += f"  {i+1}. {st} {nome}\n"
        if len(grupos) > 10:
            texto += f"\n<i>... {len(grupos)} grupos no total</i>"
    else:
        texto += "Nenhum grupo ainda. Clique em <b>➕ Escolher grupos</b>!"
    
    rows = []
    for g in grupos[:15]:  # mostra até 15 na tela principal
        st   = "🟢" if g in ativos else "🔴"
        nome = nomes_cache.get(g) or _nome_curto_grupo(g)
        rows.append([
            btn(f"{st} {nome[:28]}", f"wa_toggle_{g}"),
            btn("🗑️", f"wa_confirm_del_{g}"),
        ])
    if len(grupos) > 15:
        rows.append([btn(f"➕ Ver todos os {len(grupos)} grupos", "wa_listar")])
    rows.append([btn("➕ Escolher meus grupos", "wa_listar")])
    rows.append([btn("🔄 Atualizar grupos", "wa_listar")])
    if conectado:
        rows.append([btn("🚪 Desconectar", "wa_confirm_logout")])
    else:
        rows.append([btn("📱 Reconectar WhatsApp", "wa_connect"),
                     btn("🚪 Desconectar", "wa_confirm_logout")])
    rows += VOLTAR_MAIN
    await reply(update, texto, InlineKeyboardMarkup(rows), "HTML")
    
    
async def tela_afiliados(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    a   = db.get_assinante(uid)
    texto = (
        "🔗 <b>Seus Códigos de Afiliado</b>\n\n"
        "✅ Inseridos automaticamente nos links!\n"
        "💡 Você recebe a comissão em cada venda.\n\n"
    )
    rows = []
    for plat in ["shopee","amazon","mercadolivre","hotmart","kiwify","monetizze","magalu","aliexpress"]:
        codigo = a.get(f"aff_{plat}", "") if a else ""
        emoji  = cfg.PLATAFORMAS.get(plat, {}).get("emoji", "🔗")
        status = f"✅ <code>{codigo}</code>" if codigo else "❌ não configurado"
        texto += f"{emoji} <b>{plat.capitalize()}:</b> {status}\n"
        rows.append([btn(f"{emoji} Editar {plat.capitalize()}", f"aff_edit_{plat}")])
    rows += VOLTAR_MAIN
    await reply(update, texto, InlineKeyboardMarkup(rows), "HTML")
    
    
async def tela_auto(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    a   = db.get_assinante(uid)
    
    modo_auto    = bool(a.get("modo_auto", 0)) if a else False
    min_desconto = a.get("min_desconto", 20) if a else 20
    cat          = a.get("categoria_auto","todos") if a else "todos"
    cat_nome     = CATEGORIAS_AUTO.get(cat, cat)
    status_emoji = "🟢 ATIVO" if modo_auto else "🔴 INATIVO"
    tem_shopee   = bool(cfg.SHOPEE_APP_ID and cfg.SHOPEE_SECRET)
    canais_tg    = a.get("canais_tg",[]) if a else []
    ativos_tg    = a.get("canais_tg_ativos",[]) if a else []
    grupos_wa    = a.get("grupos_wa",[]) if a else []
    ativos_wa    = a.get("grupos_wa_ativos",[]) if a else []
    ultimo = a.get("ultimo_auto_post","") if a else ""
    proximo_str = "agora"
    if ultimo:
        try:
            diff = (datetime.now() - datetime.fromisoformat(ultimo)).total_seconds()
            mins = max(0, cfg.AUTO_POSTER_INTERVALO - int(diff/60))
            proximo_str = f"{mins} min" if mins > 0 else "agora"
        except Exception:
            pass
    
    texto = (
        f"🤖 <b>Modo Automático</b>  –  {status_emoji}\n\n"
        f"Busca promoções da Shopee e posta automaticamente!\n\n"
        f"⚙️ <b>Configuração:</b>\n"
        f"  🏷️ Desconto mínimo: <b>{min_desconto}%</b>\n"
        f"  🌐 Categoria: <b>{cat_nome}</b>\n"
        f"  ⏱️ Intervalo: <b>{cfg.AUTO_POSTER_INTERVALO} min</b>\n"
        f"  ⏩ Próximo post: <b>{proximo_str}</b>\n"
        f"  🔑 API Shopee: {'✅' if tem_shopee else '❌ Configurar no .env'}\n\n"
        f"📢 TG ativos: <b>{len(ativos_tg)}/{len(canais_tg)}</b>  "
        f"📲 WA ativos: <b>{len(ativos_wa)}/{len(grupos_wa)}</b>"
    )
    btn_toggle = (btn("🔴 Desativar","auto_off") if modo_auto else btn("🟢 Ativar","auto_on"))
    rows = [
        [btn_toggle],
        [btn("🏷️ Mín 10%","auto_min_10"),
         btn("🏷️ Mín 20%","auto_min_20"),
         btn("🏷️ Mín 30%","auto_min_30")],
        [btn("🌐 Categoria do Auto-post","menu_cat_auto")],
        [btn("📢 Config TG","menu_canais_tg"),
         btn("📲 Config WA","menu_grupos_wa")],
    ] + VOLTAR_MAIN
    await reply(update, texto, InlineKeyboardMarkup(rows), "HTML")
    
    
async def tela_categoria_auto(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    a   = db.get_assinante(uid)
    atual = a.get("categoria_auto","todos") if a else "todos"
    texto = "🌐 <b>Categoria do Auto-poster</b>\n\nEscolha quais produtos o bot busca:"
    rows  = [[btn(("✅ " if k == atual else "") + v, f"cat_auto_{k}")]
             for k, v in CATEGORIAS_AUTO.items()]
    rows += VOLTAR_MAIN
    await reply(update, texto, InlineKeyboardMarkup(rows), "HTML")
    
    
async def tela_links(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    
    # 🔥 VERIFICA ASSINATURA
    if not db.assinatura_ativa(uid):
        await reply(update, 
            "❌ <b>Assinatura inativa!</b>\n\nUse /start para renovar.",
            kb(VOLTAR_MAIN[0]), "HTML")
        return
    
    links = db.listar_links(uid)
    if not links:
        await reply(update,
            "📚 <b>Biblioteca de Links</b>\n\nVazia.\n"
            "<i>Envie um link e clique em 💾 Salvar para adicionar.</i>",
            kb(VOLTAR_MAIN[0]), "HTML")
        return
    
    texto = f"📚 <b>Biblioteca de Links</b> ({len(links)}/50)\n\n"
    rows = []
    for lk in links[:10]:
        emoji = cfg.PLATAFORMAS.get(lk["plataforma"],{}).get("emoji","🛍️")
        preco_str = f" R${lk['preco']}" if lk.get("preco") else ""
        titulo = (lk.get("titulo") or "Sem título")[:25]
        texto += f"{emoji} <b>[{lk['id']}]</b> {titulo}...{preco_str}\n"
        rows.append([
            btn(f"📤 Nicho", f"lk_post_{lk['id']}"),
            btn("🌐 Todos", f"lk_post_force_{lk['id']}"),
            btn("⏰", f"lk_agendar_{lk['id']}"),
            btn("🗑️", f"lk_confirm_del_{lk['id']}"),
        ])
    if len(links) > 10:
        texto += f"\n<i>+{len(links)-10} links (mostrando 10)</i>"
    rows += VOLTAR_MAIN
    await reply(update, texto, InlineKeyboardMarkup(rows), "HTML")
    
async def tela_agenda(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid       = update.effective_user.id
    pendentes = db.listar_agendamentos(uid)
    if not pendentes:
        await reply(update, "⏰ <b>Agendamentos</b>\n\nNenhuma postagem agendada.",
                    kb(VOLTAR_MAIN[0]), "HTML"); return
    texto = f"⏰ <b>Agendamentos Pendentes</b> ({len(pendentes)})\n\n"
    rows  = []
    for ag in pendentes:
        titulo   = (ag.get("titulo") or "Produto")[:22]
        destinos = ag.get("destinos","telegram")
        dest_icon = "📢📲" if ("telegram" in destinos and "whatsapp" in destinos) else \
                    "📲" if "whatsapp" in destinos else "📢"
        texto += f"🕐 <b>{ag['horario']}</b> {dest_icon} {titulo}...\n"
        rows.append([btn(f"❌ Cancelar {ag['horario']} – {titulo[:15]}",
                        f"ag_confirm_del_{ag['id']}")])
    rows += VOLTAR_MAIN
    await reply(update, texto, InlineKeyboardMarkup(rows), "HTML")
    
    
async def tela_estilo(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid          = update.effective_user.id
    a            = db.get_assinante(uid)
    estilo_atual = a.get("estilo","padrao") if a else "padrao"
    tmpl_ativo   = db.get_template_ativo(uid)
    templates    = db.get_templates_custom(uid)
    
    # Determina o que está selecionado atualmente
    if tmpl_ativo >= 0 and 0 <= tmpl_ativo < len(templates):
        atual_nome = f"📝 {templates[tmpl_ativo]['nome']}"
    else:
        atual_nome = ESTILOS.get(estilo_atual, {}).get("nome", "Padrão")
    
    texto = (
        f"🎨 <b>Estilo de Copy</b>\n\n"
        f"Ativo: <b>{atual_nome}</b>\n\n"
        f"<b>Estilos automáticos:</b>"
    )
    rows = []
    # Estilos padrão
    for k, v in ESTILOS.items():
        ativo = tmpl_ativo < 0 and estilo_atual == k
        rows.append([btn(("✅ " if ativo else "") + v["nome"], f"estilo_{k}")])
    
    # Templates personalizados como estilos
    if templates:
        rows.append([btn("─── 📝 Seus Templates ───", "noop")])
        for i, t in enumerate(templates):
            ativo = tmpl_ativo == i
            rows.append([
                btn(("✅ " if ativo else "📝 ") + t["nome"][:28], f"estilo_tmpl_{i}"),
                btn("🗑️", f"tmpl_del_{i}"),
            ])
    
    rows.append([btn("➕ Criar Copy Personalizada", "criar_copy_estilo")])
    rows += VOLTAR_MAIN
    await reply(update, texto, InlineKeyboardMarkup(rows), "HTML")
    
    
    
async def tela_aplicar_copy_destinos(update: Update, ctx: ContextTypes.DEFAULT_TYPE,
                                          tipo_copy: str, copy_ref: str):
    """
    Após selecionar um estilo ou template, mostra canais e grupos
    para o usuário escolher onde aplicar aquela copy.
    
    tipo_copy: "estilo_{nome}" ou "tmpl_{idx}"
    copy_ref: identificador da copy selecionada
    """
    uid      = update.effective_user.id
    a        = db.get_assinante(uid)
    if not a:
        await reply(update, "❌ Sem assinatura.", kb(VOLTAR_MAIN[0])); return
    
    canais   = a.get("canais_tg", [])
    ativos_tg = a.get("canais_tg_ativos", [])
    grupos   = a.get("grupos_wa", [])
    ativos_wa = a.get("grupos_wa_ativos", [])
    templates = db.get_templates_custom(uid)
    chats_db  = {c["chat_id"]: c["titulo"] for c in db.listar_bot_chats()}
    nomes_wa  = db.get_nomes_grupos_wa(uid)
    templates_tg = db.get_templates_tg(uid)
    templates_wa = db.get_templates_wa(uid)
    tmpl_global  = db.get_template_ativo(uid)
    estilo_g     = a.get("estilo", "padrao")
    
    # Nome amigável da copy selecionada
    if tipo_copy == "estilo":
        copy_nome = ESTILOS.get(copy_ref, {}).get("nome", copy_ref)
        copy_label = f"🎨 {copy_nome}"
    else:
        idx = int(copy_ref)
        copy_nome = templates[idx]["nome"] if 0 <= idx < len(templates) else "?"
        copy_label = f"📝 {copy_nome}"
    
    # Salva referência no contexto para os callbacks de seleção
    ctx.user_data["copy_aplicar_tipo"] = tipo_copy
    ctx.user_data["copy_aplicar_ref"]  = copy_ref
    
    texto = (
        f"✍️ <b>Aplicar Copy: {copy_label}</b>\n\n"
        f"Escolha onde aplicar esta copy:\n"
        f"(✅ = já usa esta copy  |  clique para alternar)\n\n"
    )
    rows = []
    
    # Opção: aplicar em TODOS
    rows.append([btn(f"🌐 Aplicar em TODOS os destinos",
                     f"copy_apply_all_{tipo_copy}_{copy_ref}")])
    rows.append([btn("── 📢 Canais Telegram ──", "noop")])
    
    for c in canais:
        nome_c = chats_db.get(c) or c[:22]
        st     = "🟢" if c in ativos_tg else "🔴"
        # Verifica se este canal já usa esta copy
        tmpl_c = templates_tg.get(c, -1)
        if tipo_copy == "estilo":
            ja_usa = tmpl_c < 0 and estilo_g == copy_ref
        else:
            ja_usa = tmpl_c == int(copy_ref)
        check = "✅ " if ja_usa else ""
        rows.append([btn(f"{check}{st} {nome_c[:30]}",
                         f"copy_apply_tg_{c}_{tipo_copy}_{copy_ref}")])
    
    if grupos:
        rows.append([btn("── 📲 Grupos WhatsApp ──", "noop")])
        for g in grupos:
            nome_g = nomes_wa.get(g) or _nome_curto_grupo(g)
            st     = "🟢" if g in ativos_wa else "🔴"
            tmpl_g = templates_wa.get(g, -1)
            if tipo_copy == "estilo":
                ja_usa = tmpl_g < 0 and estilo_g == copy_ref
            else:
                ja_usa = tmpl_g == int(copy_ref)
            check = "✅ " if ja_usa else ""
            rows.append([btn(f"{check}{st} {nome_g[:30]}",
                             f"copy_apply_wa_{g}_{tipo_copy}_{copy_ref}")])
    
    rows.append([btn("🔙 Voltar", "menu_estilo")])
    await reply(update, texto, InlineKeyboardMarkup(rows), "HTML")
    
    
async def tela_nichos(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Tela principal de nichos — suporta múltiplos nichos por canal/grupo."""
    uid = update.effective_user.id
    a   = db.get_assinante(uid)
    if not a:
        await reply(update, "❌ Sem assinatura.", kb(VOLTAR_MAIN[0])); return

    canais    = a.get("canais_tg", [])
    grupos    = a.get("grupos_wa", [])
    nichos_tg = db.get_nichos_tg(uid)
    nichos_wa = db.get_nichos_wa(uid)

    # Conta quantos destinos têm cada nicho (um destino pode contar em vários)
    resumo: Dict[str, int] = {}
    for c in canais:
        for n in nichos_tg.get(c, []):
            resumo[n] = resumo.get(n, 0) + 1
    for g in grupos:
        for n in nichos_wa.get(g, []):
            resumo[n] = resumo.get(n, 0) + 1

    texto = (
        "🌐 <b>Nichos por Canal/Grupo</b>\n\n"
        "💡 <b>Novidade: múltiplos nichos por canal!</b>\n"
        "Cada canal/grupo pode receber produtos de vários nichos.\n"
        "Selecione um nicho abaixo para ativar/desativar:\n\n"
    )
    rows = []
    for k, v in CATEGORIAS_AUTO.items():
        if k == "todos":
            continue
        qtd   = resumo.get(k, 0)
        badge = f" ✅{qtd}" if qtd else ""
        rows.append([btn(f"{v}{badge}", f"nicho_ver_{k}")])

    rows += VOLTAR_MAIN
    await reply(update, texto, InlineKeyboardMarkup(rows), "HTML")
    
    
async def tela_nicho_destinos(update: Update, ctx: ContextTypes.DEFAULT_TYPE,
                                   nicho: str, pagina: int = 0):
    """
    Mostra canais e grupos com opção de toggle do nicho.
    Nichos ativos ficam no texto (não no botão) para evitar truncamento.
    Suporta paginação quando há muitos canais/grupos.
    """
    uid       = update.effective_user.id
    a         = db.get_assinante(uid)
    if not a:
        await reply(update, "❌ Sem assinatura.", kb(VOLTAR_MAIN[0])); return

    canais    = a.get("canais_tg", [])
    ativos_tg = a.get("canais_tg_ativos", [])
    grupos    = a.get("grupos_wa", [])
    ativos_wa = a.get("grupos_wa_ativos", [])
    nichos_tg = db.get_nichos_tg(uid)
    nichos_wa = db.get_nichos_wa(uid)
    chats_db  = {c["chat_id"]: c["titulo"] for c in db.listar_bot_chats()}
    nomes_wa  = db.get_nomes_grupos_wa(uid)

    nicho_nome = CATEGORIAS_AUTO.get(nicho, nicho)

    # ── Monta texto com estado de cada destino ────────────────
    linhas_texto = []

    if canais:
        linhas_texto.append("📢 <b>Canais Telegram:</b>")
        for c in canais:
            nome_c  = (chats_db.get(c) or c)[:28]
            st      = "🟢" if c in ativos_tg else "🔴"
            lista   = nichos_tg.get(c, [])
            ja_usa  = nicho in lista
            check   = "✅" if ja_usa else "☐"
            if lista:
                nomes_nichos = ", ".join(
                    CATEGORIAS_AUTO.get(n, n).split(" ", 1)[-1]  # remove emoji do nome
                    for n in lista
                )
                linhas_texto.append(f"  {check} {st} <b>{nome_c}</b>\n       📌 {nomes_nichos}")
            else:
                linhas_texto.append(f"  {check} {st} <b>{nome_c}</b>  (Todos)")

    if grupos:
        linhas_texto.append("\n📲 <b>Grupos WhatsApp:</b>")
        for g in grupos:
            nome_g  = (nomes_wa.get(g) or _nome_curto_grupo(g))[:28]
            st      = "🟢" if g in ativos_wa else "🔴"
            lista   = nichos_wa.get(g, [])
            ja_usa  = nicho in lista
            check   = "✅" if ja_usa else "☐"
            if lista:
                nomes_nichos = ", ".join(
                    CATEGORIAS_AUTO.get(n, n).split(" ", 1)[-1]
                    for n in lista
                )
                linhas_texto.append(f"  {check} {st} <b>{nome_g}</b>\n       📌 {nomes_nichos}")
            else:
                linhas_texto.append(f"  {check} {st} <b>{nome_g}</b>  (Todos)")

    texto = (
        f"🌐 <b>Nicho: {nicho_nome}</b>\n\n"
        f"Clique no canal/grupo para ativar ✅ ou remover ☐ este nicho.\n"
        f"Um destino pode ter <b>vários nichos</b> ao mesmo tempo.\n\n"
        + ("\n".join(linhas_texto) if linhas_texto
           else "<i>Nenhum canal ou grupo configurado.</i>")
    )

    # ── Botões de ação — simples, sem nichos no label ─────────
    POR_PAG = 8  # canais+grupos por página
    todos_destinos = (
        [("tg", c, chats_db.get(c) or c, c in ativos_tg, nichos_tg.get(c, []))
         for c in canais] +
        [("wa", g, nomes_wa.get(g) or _nome_curto_grupo(g), g in ativos_wa, nichos_wa.get(g, []))
         for g in grupos]
    )
    total      = len(todos_destinos)
    inicio     = pagina * POR_PAG
    fim        = inicio + POR_PAG
    pag_dest   = todos_destinos[inicio:fim]
    total_pags = max(1, (total + POR_PAG - 1) // POR_PAG)

    rows = []
    rows.append([btn(f"✅ Adicionar {nicho_nome} em TODOS", f"nicho_apply_all_{nicho}")])
    rows.append([btn(f"🗑️ Remover {nicho_nome} de TODOS",  f"nicho_remove_all_{nicho}")])

    if pag_dest:
        rows.append([btn("─────────────────────", "noop")])

    for tipo, dest_id, nome, ativo, lista in pag_dest:
        ja_usa = nicho in lista
        st     = "🟢" if ativo else "🔴"
        check  = "✅" if ja_usa else "☐ "
        # Label curto e limpo — sem lista de nichos (está no texto acima)
        label  = f"{check} {st} {nome[:30]}"
        cb     = f"nicho_toggle_{tipo}_{dest_id}_{nicho}"
        rows.append([btn(label, cb)])

    # Paginação
    if total_pags > 1:
        nav = []
        if pagina > 0:
            nav.append(btn("◀️", f"nicho_pag_{nicho}_{pagina - 1}"))
        nav.append(btn(f"{pagina + 1}/{total_pags}", "noop"))
        if pagina < total_pags - 1:
            nav.append(btn("▶️", f"nicho_pag_{nicho}_{pagina + 1}"))
        rows.append(nav)

    rows.append([btn("🔙 Voltar aos Nichos", "menu_nichos")])
    await reply(update, texto, InlineKeyboardMarkup(rows), "HTML")
    
async def tela_referral(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    link = f"https://t.me/{ctx.bot.username}?start=ref_{uid}"
    refs = db.stats_referral(uid)
    texto = (
        f"🎁 <b>Programa de Indicações</b>\n\n"
        f"🔗 Seu link único:\n<code>{link}</code>\n\n"
        f"┣ Amigo assina → <b>você ganha +{cfg.BONUS_CONVIDANTE} dias</b>\n"
        f"┗ Amigo recebe <b>+{cfg.BONUS_CONVIDADO} dias extras</b> 🎉\n\n"
        f"✅ Recompensadas: <b>{refs['total']}</b>\n"
        f"⏳ Pendentes: <b>{refs['pendentes']}</b>"
    )
    await reply(update, texto, kb(VOLTAR_MAIN[0]), "HTML")
    
    
async def tela_config(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid   = update.effective_user.id
    a     = db.get_assinante(uid)
    email = (a.get("email") or "Não cadastrado") if a else "N/A"
    estilo = ESTILOS.get(a.get("estilo","padrao") if a else "padrao",{}).get("nome","Padrão")
    bridge = (a.get("wa_bridge_url","") or cfg.WA_BRIDGE_URL) if a else cfg.WA_BRIDGE_URL
    plano  = PLANOS.get(a.get("plano","mensal") if a else "mensal",{}).get("nome","Mensal")
    texto = (
        f"⚙️ <b>Configurações</b>\n\n"
        f"📧 Email: <code>{email}</code>\n"
        f"🎨 Estilo: <b>{estilo}</b>\n"
        f"📋 Plano: <b>{plano}</b>\n"
        f"🌐 Bridge WA: <code>{bridge}</code>"
    )
    await reply(update, texto,
        kb([btn("📧 Alterar Email",   "config_email")],
           [btn("🎨 Alterar Estilo",  "menu_estilo")],
           [btn("💳 Renovar Assinatura", "ver_planos")],
           VOLTAR_MAIN[0]), "HTML")
    
    
async def tela_ajuda(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    texto = (
        "❓ <b>Central de Ajuda</b>\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "📤 <b>Postar produto:</b>\n"
        "Envie o link → o bot extrai dados e gera a copy!\n\n"
        "📢 <b>Canais Telegram:</b>\n"
        "📢 Canais TG → ➕ Adicionar → selecione na lista\n\n"
        "📲 <b>WhatsApp:</b>\n"
        "1. Inicie a bridge: <code>node whatsapp-bridge.js</code>\n"
        "2. Conecte com código de pareamento\n"
        "3. Escolha seus grupos na lista\n\n"
        "🤖 <b>Modo Automático:</b>\n"
        f"Posta promoções a cada {cfg.AUTO_POSTER_INTERVALO}min automaticamente.\n"
        "Configure desconto mínimo e categoria!\n\n"
        "🔗 <b>Afiliados:</b>\n"
        "Configure em 🔗 Afiliados. Comissão vai <b>para você</b>!\n\n"
        "📝 <b>Templates Personalizados:</b>\n"
        "Crie seus próprios textos de postagem.\n\n"
        "🚫 <b>Blacklist:</b>\n"
        "Bloqueie lojas ou palavras no auto-poster.\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "Comandos: /start /status /meuid /email /ajuda"
    )
    await reply(update, texto,
        kb([btn_url("🆘 Suporte", cfg.SUPORTE_LINK)], VOLTAR_MAIN[0]), "HTML")
    
    
    # ══════════════════════════════════════════════════════════════
    #  AÇÕES DE POSTAGEM
    # ══════════════════════════════════════════════════════════════
    
def _filtrar_destinos_por_nicho(uid: int, produto: "Produto",
                                      canais_tg: List[str], grupos_wa: List[str]
                                      ) -> tuple:
    """
    Filtra canais TG e grupos WA pelo nicho configurado.
    - Canais/grupos com nicho "todos" → sempre incluídos
    - Canais/grupos com nicho específico → só se o produto bater
    Retorna (canais_filtrados, grupos_filtrados, info_filtro)
    """
    nichos_tg = db.get_nichos_tg(uid)
    nichos_wa = db.get_nichos_wa(uid)
    
    canais_ok  = []
    canais_skip = []
    for c in canais_tg:
        nicho = nichos_tg.get(c, "todos")
        if nicho == "todos" or _produto_bate_nicho(produto, nicho):
            canais_ok.append(c)
        else:
            canais_skip.append((c, nicho))
    
    grupos_ok  = []
    grupos_skip = []
    for g in grupos_wa:
        nicho = nichos_wa.get(g, "todos")
        if nicho == "todos" or _produto_bate_nicho(produto, nicho):
            grupos_ok.append(g)
        else:
            grupos_skip.append((g, nicho))
    
    info = ""
    if canais_skip:
        nomes = ", ".join(f"{n}" for _, n in canais_skip[:3])
        info += f"\n⚠️ {len(canais_skip)} canal(is) pulado(s) por nicho ({nomes})"
    if grupos_skip:
        nomes = ", ".join(f"{n}" for _, n in grupos_skip[:3])
        info += f"\n⚠️ {len(grupos_skip)} grupo(s) pulado(s) por nicho ({nomes})"
    
    return canais_ok, grupos_ok, info
    
async def executar_postagem_telegram(update: Update, ctx: ContextTypes.DEFAULT_TYPE, url_hash: str):
    uid     = update.effective_user.id
    produto = ctx.user_data.get(f"prod_{url_hash}")
    if not produto:
        await reply(update, "⏳ Sessão expirada. Reenvie o link.", kb(VOLTAR_MAIN[0])); return
    a = db.get_assinante(uid)
    canais_ativos = [c for c in a.get("canais_tg",[]) if c in a.get("canais_tg_ativos",[])] if a else []
    if not canais_ativos:
        canais_ativos = a.get("canais_tg",[]) if a else []
    if not canais_ativos:
        await reply(update, "⚠️ Nenhum canal Telegram configurado!",
                    kb([btn("📢 Configurar","menu_canais_tg")],VOLTAR_MAIN[0]),"HTML"); return
    # Filtra canais pelo nicho do produto
    canais, _, info_nicho = _filtrar_destinos_por_nicho(uid, produto, canais_ativos, [])
    if not canais:
        await reply(update,
            f"⚠️ Nenhum canal compatível com o nicho deste produto.\n{info_nicho}\n\n"
            f"Configure <b>🌐 Todos</b> em algum canal para receber qualquer produto.",
            kb([btn("📢 Canais","menu_canais_tg")],VOLTAR_MAIN[0]),"HTML"); return
    postador = Postador(ctx.bot)
    msg      = await update.effective_message.reply_text("📢 Postando no Telegram... ⏳")
    linhas   = [f"✅ <b>Telegram</b>{info_nicho}\n"]
    for canal in canais:
        ok, erro = await postador.postar(uid, produto, canal, copy)
        linhas.append(f"  {'✅' if ok else '❌'} <code>{canal}</code>"
                      + (f"\n  <i>{erro[:60]}</i>" if not ok else ""))
        if ok:
            db.log_postagem(uid, url_hash, canal, True, metodo=produto.metodo, destino="telegram",
                            url=produto.link.url_original, titulo=produto.titulo,
                            imagem=produto.imagem, preco=produto.preco,
                            desconto=produto.desconto_pct)
            db.inc_postagem(uid, "telegram")
    try:
        await msg.edit_text("\n".join(linhas), parse_mode="HTML",
                            reply_markup=kb(VOLTAR_MAIN[0]))
    except Exception:
        await update.effective_message.reply_text("\n".join(linhas), parse_mode="HTML",
                                                  reply_markup=kb(VOLTAR_MAIN[0]))
    
async def executar_postagem_whatsapp(update: Update, ctx: ContextTypes.DEFAULT_TYPE, url_hash: str):
    uid     = update.effective_user.id
    produto = ctx.user_data.get(f"prod_{url_hash}")
    if not produto:
        await reply(update, "⏳ Sessão expirada. Reenvie o link.", kb(VOLTAR_MAIN[0])); return
    a      = db.get_assinante(uid)
    grupos_ativos = [g for g in a.get("grupos_wa",[]) if g in a.get("grupos_wa_ativos",[])] if a else []
    if not grupos_ativos:
        grupos_ativos = a.get("grupos_wa",[]) if a else []
    if not grupos_ativos:
        await reply(update, "⚠️ Nenhum grupo WhatsApp configurado!",
                    kb([btn("📲 Configurar","menu_grupos_wa")],VOLTAR_MAIN[0]),"HTML"); return
    # Filtra grupos pelo nicho do produto
    _, grupos, info_nicho = _filtrar_destinos_por_nicho(uid, produto, [], grupos_ativos)
    if not grupos:
        await reply(update,
            f"⚠️ Nenhum grupo compatível com o nicho deste produto.\n{info_nicho}\n\n"
            f"Configure <b>🌐 Todos</b> em algum grupo para receber qualquer produto.",
            kb([btn("📲 Grupos","menu_grupos_wa")],VOLTAR_MAIN[0]),"HTML"); return
    bridge = (a.get("wa_bridge_url","") or cfg.WA_BRIDGE_URL) if a else cfg.WA_BRIDGE_URL
    msg    = await update.effective_message.reply_text("📲 Enviando para WhatsApp... ⏳")
    if not await wa_bridge_online(bridge):
        await msg.edit_text("❌ <b>Bridge WhatsApp offline!</b>\n\nInicie: <code>node whatsapp-bridge.js</code>",
                            parse_mode="HTML", reply_markup=kb(VOLTAR_MAIN[0])); return
    
    estilo = a.get('estilo', 'padrao') if a else 'padrao'
    copy = gerar_copy(produto, estilo)
    
    ok, erros, enviados = await postar_whatsapp(uid, produto, copy, grupos, bridge)
    if ok:
        db.inc_postagem(uid, "whatsapp")
        db.log_postagem(uid, url_hash, "wa_groups", True, metodo=produto.metodo, destino="whatsapp",
                        url=produto.link.url_original, titulo=produto.titulo,
                        imagem=produto.imagem, preco=produto.preco, desconto=produto.desconto_pct)
        await msg.edit_text(f"✅ <b>WhatsApp!</b> {enviados}/{len(grupos)} grupo(s)",
                            parse_mode="HTML", reply_markup=kb(VOLTAR_MAIN[0]))
    else:
        await msg.edit_text(f"❌ <b>Falha WA</b>\n<i>{erros[:200]}</i>",
                            parse_mode="HTML", reply_markup=kb(VOLTAR_MAIN[0]))
    
async def executar_postagem_ambos(update: Update, ctx: ContextTypes.DEFAULT_TYPE, url_hash: str):
    uid     = update.effective_user.id
    produto = ctx.user_data.get(f"prod_{url_hash}")
    if not produto:
        await reply(update, "⏳ Sessão expirada. Reenvie o link.", kb(VOLTAR_MAIN[0])); return
    a        = db.get_assinante(uid)
    copy     = (db.get_copy_custom(uid, url_hash) or
               ctx.user_data.get(f"copy_custom_{url_hash}") or
               gerar_copy(produto, a.get("estilo","padrao") if a else "padrao"))
    canais_ativos = [c for c in a.get("canais_tg",[]) if c in a.get("canais_tg_ativos",[])] if a else []
    grupos_ativos = [g for g in a.get("grupos_wa",[]) if g in a.get("grupos_wa_ativos",[])] if a else []
    # Filtra por nicho do produto
    canais, grupos, info_nicho = _filtrar_destinos_por_nicho(uid, produto, canais_ativos, grupos_ativos)
    bridge   = (a or {}).get("wa_bridge_url","") or cfg.WA_BRIDGE_URL
    msg      = await update.effective_message.reply_text("🚀 Postando em tudo... ⏳")
    resultados = [f"📊 <b>Resultado</b>{info_nicho}\n"]
    postador   = Postador(ctx.bot)
    if canais:
        resultados.append("📢 <b>Telegram:</b>")
        for canal in canais:
            ok, erro = await postador.postar(uid, produto, canal, copy)
            resultados.append(f"  {'✅' if ok else '❌'} <code>{canal}</code>")
            if ok:
                db.inc_postagem(uid, "telegram")
                db.log_postagem(uid, url_hash, canal, True, metodo=produto.metodo, destino="telegram",
                                url=produto.link.url_original, titulo=produto.titulo,
                                imagem=produto.imagem, preco=produto.preco,
                                desconto=produto.desconto_pct)
    else:
        resultados.append("⚠️ Telegram: sem canal ativo")
    if grupos:
        resultados.append("\n📲 <b>WhatsApp:</b>")
        if not await wa_bridge_online(bridge):
            resultados.append("  ❌ Bridge offline")
        else:
            ok_wa, _, enviados = await postar_whatsapp(uid, produto, copy, grupos, bridge)
            resultados.append(f"  {'✅' if ok_wa else '❌'} {enviados}/{len(grupos)} grupo(s)")
            if ok_wa:
                db.inc_postagem(uid, "whatsapp")
                db.log_postagem(uid, url_hash, "wa_groups", True, metodo=produto.metodo,
                                destino="whatsapp", url=produto.link.url_original,
                                titulo=produto.titulo, imagem=produto.imagem,
                                preco=produto.preco, desconto=produto.desconto_pct)
    else:
        resultados.append("\n⚠️ WhatsApp: sem grupo ativo")
    try:
        await msg.edit_text("\n".join(resultados), parse_mode="HTML",
                            reply_markup=kb(VOLTAR_MAIN[0]))
    except Exception:
        await update.effective_message.reply_text("\n".join(resultados), parse_mode="HTML",
                                                  reply_markup=kb(VOLTAR_MAIN[0]))
    
    
    
async def executar_postagem_force(update: Update, ctx: ContextTypes.DEFAULT_TYPE,
                                       url_hash: str, destino: str = "ambos"):
    """Posta em TODOS os canais/grupos ativos sem verificar nicho."""
    uid     = update.effective_user.id
    produto = ctx.user_data.get(f"prod_{url_hash}")
    if not produto:
        await reply(update, "⏳ Sessão expirada. Reenvie o link.", kb(VOLTAR_MAIN[0])); return
    a       = db.get_assinante(uid)
    canais  = [c for c in a.get("canais_tg",[]) if c in a.get("canais_tg_ativos",[])] if a else []
    grupos  = [g for g in a.get("grupos_wa",[]) if g in a.get("grupos_wa_ativos",[])] if a else []
    bridge  = (a or {}).get("wa_bridge_url","") or cfg.WA_BRIDGE_URL
    # Não salva copy no sistema automático — apenas para este post manual
    copy    = (db.get_copy_custom(uid, url_hash) or
               ctx.user_data.get(f"copy_custom_{url_hash}") or
               gerar_copy(produto, a.get("estilo","padrao") if a else "padrao"))
    msg     = await update.effective_message.reply_text("🌐 Postando em todos... ⏳")
    linhas  = ["🌐 <b>Postando em TODOS (sem filtro de nicho)</b>\n"]
    postador = Postador(ctx.bot)
    if destino in ("tg","ambos") and canais:
        linhas.append("📢 <b>Telegram:</b>")
        for canal in canais:
            ok, _ = await postador.postar(uid, produto, canal, copy)
            linhas.append(f"  {'✅' if ok else '❌'} <code>{canal}</code>")
            if ok:
                db.inc_postagem(uid, "telegram")
                db.log_postagem(uid, url_hash, canal, True, metodo=produto.metodo,
                                destino="telegram", url=produto.link.url_original,
                                titulo=produto.titulo, imagem=produto.imagem,
                                preco=produto.preco, desconto=produto.desconto_pct)
    if destino in ("wa","ambos") and grupos:
        linhas.append("\n📲 <b>WhatsApp:</b>")
        if await wa_bridge_online(bridge):
            ok_wa, _, enviados = await postar_whatsapp(uid, produto, copy, grupos, bridge)
            linhas.append(f"  {'✅' if ok_wa else '❌'} {enviados}/{len(grupos)} grupos")
            if ok_wa: db.inc_postagem(uid, "whatsapp")
        else:
            linhas.append("  ❌ Bridge offline")
    try:
        await msg.edit_text("\n".join(linhas), parse_mode="HTML",
                            reply_markup=kb(VOLTAR_MAIN[0]))
    except Exception:
        await update.effective_message.reply_text("\n".join(linhas), parse_mode="HTML",
                                                  reply_markup=kb(VOLTAR_MAIN[0]))
    
async def repostar_historico(update: Update, ctx: ContextTypes.DEFAULT_TYPE, hist_id: int):
    uid = update.effective_user.id
    rows_db = db._exec(
        "SELECT * FROM historico WHERE id=%s AND user_id=%s", (hist_id, uid), fetch="one")
    if not rows_db:
        await reply(update, "❌ Post não encontrado.", kb(VOLTAR_MAIN[0])); return
    h = dict(rows_db)
    url = h.get("url", "")
    if not url:
        await reply(update, "❌ URL não disponível para repostar.", kb(VOLTAR_MAIN[0])); return
    msg = await update.effective_message.reply_text("⏳ Preparando repost...")
    try:
        link    = LinkAnalyzer.analisar(url)
        produto = await Extratores.extrair(link)
        a       = db.get_assinante(uid)
        copy    = gerar_copy(produto, a.get("estilo","padrao") if a else "padrao")
        postador = Postador(ctx.bot)
        canais   = [c for c in a.get("canais_tg",[]) if c in a.get("canais_tg_ativos",[])] if a else []
        grupos   = [g for g in a.get("grupos_wa",[]) if g in a.get("grupos_wa_ativos",[])] if a else []
        bridge   = (a or {}).get("wa_bridge_url","") or cfg.WA_BRIDGE_URL
        linhas = ["🔄 <b>Repostado!</b>\n"]
        for canal in canais:
            ok, _ = await postador.postar(uid, produto, canal, copy)
            linhas.append(f"  {'✅' if ok else '❌'} TG <code>{canal}</code>")
            if ok: db.inc_postagem(uid, "telegram")
        if grupos and await wa_bridge_online(bridge):
            ok_wa, _, enviados = await postar_whatsapp(uid, produto, copy, grupos, bridge)
            linhas.append(f"  {'✅' if ok_wa else '❌'} WA {enviados}/{len(grupos)}")
            if ok_wa: db.inc_postagem(uid, "whatsapp")
        await msg.edit_text("\n".join(linhas), parse_mode="HTML",
                            reply_markup=kb(VOLTAR_MAIN[0]))
    except Exception as e:
        await msg.edit_text(f"❌ Erro ao repostar: {str(e)[:150]}", parse_mode="HTML")
    
    
async def salvar_produto(update: Update, ctx: ContextTypes.DEFAULT_TYPE, url_hash: str):
    uid     = update.effective_user.id
    produto = ctx.user_data.get(f"prod_{url_hash}")
    url     = ctx.user_data.get("ultimo_url")
    if not produto or not url:
        await reply(update, "⏳ Sessão expirada.", kb(VOLTAR_MAIN[0])); return
    links = db.listar_links(uid)
    if len(links) >= 50:
        await reply(update, "❌ Limite de 50 links atingido!",
                    kb([btn("📚 Biblioteca","menu_links")],VOLTAR_MAIN[0]),"HTML"); return
    db.salvar_link(uid, url, produto.titulo, produto.link.plataforma,
                   url_hash, produto.preco, produto.imagem, produto.video)
    await reply(update,
        f"✅ <b>Salvo!</b>\n🛍️ {produto.titulo[:60]}\n<i>Total: {len(links)+1}/50</i>",
        kb([btn("📚 Biblioteca","menu_links")],VOLTAR_MAIN[0]),"HTML")
    
    
    
async def tela_editar_copy(update: Update, ctx: ContextTypes.DEFAULT_TYPE, url_hash: str):
    """Permite ao usuário digitar sua própria copy antes de postar."""
    uid     = update.effective_user.id
    produto = ctx.user_data.get(f"prod_{url_hash}")
    if not produto:
        await reply(update, "⏳ Sessão expirada. Reenvie o link.", kb(VOLTAR_MAIN[0]))
        return
    ctx.user_data["aguardando"]       = "copy_customizada"
    ctx.user_data["hash_edit_copy"]   = url_hash
    titulo = produto.titulo[:60]
    preco  = f"R$ {produto.preco}" if produto.preco else ""
    link_final = aplicar_afiliado(uid, produto.link.plataforma,
                                  produto.link.url_original, produto.link_afiliado)
    await reply(update,
        f"✏️ <b>Editar Copy</b>\n\n"
        f"📦 <b>{titulo}</b>\n"
        f"{'💰 ' + preco if preco else ''}\n\n"
        f"Digite sua copy personalizada abaixo.\n"
        f"O link de compra é adicionado automaticamente.\n\n"
        f"<b>Variáveis que você pode usar:</b>\n"
        f"  <code>{{titulo}}</code> → {titulo[:30]}\n"
        f"  <code>{{preco}}</code> → {preco}\n"
        f"  <code>{{desconto}}</code> → {produto.desconto_pct}%\n"
        f"  <code>{{loja}}</code> → {produto.loja or 'N/A'}\n\n"
        f"<i>Exemplo:</i>\n"
        f"<code>🔥 Olha essa oferta! {{titulo}} por apenas {{preco}}! Corre!</code>",
        kb([btn("❌ Cancelar", f"recopy_{url_hash}")]), "HTML")
    
async def tela_selecionar_copy(update: Update, ctx: ContextTypes.DEFAULT_TYPE, url_hash: str):
    """Tela para escolher qual estilo/template usar na postagem."""
    uid      = update.effective_user.id
    produto  = ctx.user_data.get(f"prod_{url_hash}")
    if not produto:
        await reply(update, "⏳ Sessão expirada. Reenvie o link.", kb(VOLTAR_MAIN[0])); return
    a        = db.get_assinante(uid)
    templates = db.get_templates_custom(uid)
    tmpl_ativo = db.get_template_ativo(uid)
    estilo_atual = a.get("estilo","padrao") if a else "padrao"
    
    texto = "✍️ <b>Escolher Copy</b>\n\nSelecione o estilo para esta postagem:\n"
    rows  = []
    
    # Estilos padrão
    rows.append([btn("━━ Estilos Automáticos ━━", "noop")])
    for k, v in ESTILOS.items():
        ativo = tmpl_ativo < 0 and estilo_atual == k
        rows.append([btn(("✅ " if ativo else "") + v["nome"],
                         f"usar_estilo_{url_hash}_{k}")])
    
    # Templates personalizados
    if templates:
        rows.append([btn("━━ Suas Copies ━━", "noop")])
        for i, t in enumerate(templates):
            ativo = tmpl_ativo == i
            rows.append([btn(("✅ " if ativo else "📝 ") + t["nome"][:30],
                             f"usar_tmpl_{url_hash}_{i}")])
    
    rows.append([btn("✏️ Criar nova copy", f"edit_copy_{url_hash}")])
    rows.append(VOLTAR_MAIN[0])
    await reply(update, texto, InlineKeyboardMarkup(rows), "HTML")
    
    
async def aplicar_copy_e_mostrar(update: Update, ctx: ContextTypes.DEFAULT_TYPE,
                                      url_hash: str, copy: str, nome_estilo: str):
    """Mostra preview da copy selecionada com botões de postagem."""
    uid     = update.effective_user.id
    produto = ctx.user_data.get(f"prod_{url_hash}")
    if not produto:
        await reply(update, "⏳ Sessão expirada.", kb(VOLTAR_MAIN[0])); return
    
    # Salva no banco para garantir que vai na postagem
    db.salvar_copy_custom(uid, url_hash, copy)
    ctx.user_data[f"copy_custom_{url_hash}"] = copy
    
    html_copy = re.sub(r"\*(.+?)\*", r"<b>\1</b>", copy)
    a = db.get_assinante(uid)
    canais_tg = [c for c in (a or {}).get("canais_tg",[])
                 if c in (a or {}).get("canais_tg_ativos",[])]
    grupos_wa = [g for g in (a or {}).get("grupos_wa",[])
                 if g in (a or {}).get("grupos_wa_ativos",[])]
    n_tg = len(canais_tg)
    n_wa = len(grupos_wa)
    
    rows = []
    if n_tg and n_wa:
        rows.append([btn(f"🚀 Tudo por nicho ({n_tg} TG + {n_wa} WA)", f"post_all_{url_hash}")])
        rows.append([btn(f"🌐 Postar em TODOS (ignora nicho)", f"post_all_force_{url_hash}")])
    if n_tg:
        rows.append([btn(f"📢 Telegram ({n_tg})", f"post_tg_{url_hash}"),
                     btn(f"🌐 Todos TG", f"post_tg_force_{url_hash}")])
    if n_wa:
        rows.append([btn(f"📲 WhatsApp ({n_wa})", f"post_wa_{url_hash}"),
                     btn(f"🌐 Todos WA", f"post_wa_force_{url_hash}")])
    rows.append([btn("🎯 Escolher Destino", f"dest_sel_{url_hash}")])
    rows.append([btn("✍️ Trocar copy",  f"recopy_{url_hash}"),
                 btn("✏️ Editar texto", f"edit_copy_{url_hash}")])
    rows.append([btn("💾 Salvar",       f"salvar_{url_hash}"),
                 btn("⏰ Agendar",      f"agendar_prod_{url_hash}")])
    rows.append(VOLTAR_MAIN[0])
    
    await reply(update,
        f"✅ <b>{nome_estilo}</b>\n\n{html_copy}\n\n<b>Onde postar?</b>",
        InlineKeyboardMarkup(rows), "HTML")
    
    
async def regerar_copy(update: Update, ctx: ContextTypes.DEFAULT_TYPE, url_hash: str):
    """Agora abre a tela de seleção de copy."""
    await tela_selecionar_copy(update, ctx, url_hash)

async def tela_escolher_destino(update: Update, ctx: ContextTypes.DEFAULT_TYPE, url_hash: str):
    """Tela para selecionar individualmente cada canal/grupo para postar."""
    uid     = update.effective_user.id
    produto = ctx.user_data.get(f"prod_{url_hash}")
    if not produto:
        await reply(update, "⏳ Sessão expirada. Reenvie o link.", kb(VOLTAR_MAIN[0])); return
    a         = db.get_assinante(uid)
    canais_tg = a.get("canais_tg", []) if a else []
    ativos_tg = a.get("canais_tg_ativos", []) if a else []
    grupos_wa = a.get("grupos_wa", []) if a else []
    ativos_wa = a.get("grupos_wa_ativos", []) if a else []
    chats_db  = {c["chat_id"]: c["titulo"] for c in db.listar_bot_chats()}
    nomes_wa  = db.get_nomes_grupos_wa(uid)

    # Inicializa seleção padrão na primeira vez: todos os ativos
    key_sel = f"destinos_sel_{url_hash}"
    if key_sel not in ctx.user_data:
        ctx.user_data[key_sel] = {
            "tg": [c for c in canais_tg if c in ativos_tg],
            "wa": [g for g in grupos_wa if g in ativos_wa],
        }
    sel    = ctx.user_data[key_sel]
    sel_tg = sel.get("tg", [])
    sel_wa = sel.get("wa", [])
    n_sel  = len(sel_tg) + len(sel_wa)

    texto = (
        f"🎯 <b>Escolher Destinos</b>\n\n"
        f"Toque para marcar/desmarcar onde postar:\n"
        f"✅ = selecionado  |  ⬜ = ignorado\n\n"
        f"<b>{n_sel} destino(s) marcado(s)</b>"
    )
    rows = []

    if canais_tg:
        rows.append([btn("── 📢 Canais Telegram ──", "noop")])
        for c in canais_tg:
            nome  = chats_db.get(c) or c[:22]
            st    = "🟢" if c in ativos_tg else "🔴"
            check = "✅" if c in sel_tg else "⬜"
            rows.append([btn(f"{check} {st} {nome[:30]}", f"dest_tg_{url_hash}_{c}")])
        if len(canais_tg) > 1:
            rows.append([
                btn("✅ Todos TG",  f"dest_all_tg_{url_hash}"),
                btn("⬜ Nenhum TG", f"dest_none_tg_{url_hash}"),
            ])

    if grupos_wa:
        rows.append([btn("── 📲 Grupos WhatsApp ──", "noop")])
        for g in grupos_wa:
            nome  = nomes_wa.get(g) or _nome_curto_grupo(g)
            st    = "🟢" if g in ativos_wa else "🔴"
            check = "✅" if g in sel_wa else "⬜"
            rows.append([btn(f"{check} {st} {nome[:30]}", f"dest_wa_{url_hash}_{g}")])
        if len(grupos_wa) > 1:
            rows.append([
                btn("✅ Todos WA",  f"dest_all_wa_{url_hash}"),
                btn("⬜ Nenhum WA", f"dest_none_wa_{url_hash}"),
            ])

    if n_sel > 0:
        rows.append([btn(f"🚀 Postar em {n_sel} destino(s) selecionado(s)", f"dest_confirm_{url_hash}")])
    else:
        rows.append([btn("⚠️ Selecione ao menos 1 destino", "noop")])

    rows.append([btn("🔙 Voltar", f"recopy_{url_hash}")])
    await reply(update, texto, InlineKeyboardMarkup(rows), "HTML")


async def executar_postagem_destinos_selecionados(
        update: Update, ctx: ContextTypes.DEFAULT_TYPE, url_hash: str):
    """Posta apenas nos destinos manualmente selecionados pelo usuário."""
    uid     = update.effective_user.id
    produto = ctx.user_data.get(f"prod_{url_hash}")
    if not produto:
        await reply(update, "⏳ Sessão expirada. Reenvie o link.", kb(VOLTAR_MAIN[0])); return
    a          = db.get_assinante(uid)
    sel        = ctx.user_data.get(f"destinos_sel_{url_hash}", {})
    canais_sel = sel.get("tg", [])
    grupos_sel = sel.get("wa", [])
    if not canais_sel and not grupos_sel:
        await reply(update, "⚠️ Nenhum destino selecionado!\nUse 🎯 Escolher Destino para marcar ao menos um.",
                    kb(VOLTAR_MAIN[0]), "HTML"); return

    copy = (db.get_copy_custom(uid, url_hash) or
            ctx.user_data.get(f"copy_custom_{url_hash}") or
            gerar_copy(produto, a.get("estilo", "padrao") if a else "padrao"))
    bridge   = (a or {}).get("wa_bridge_url", "") or cfg.WA_BRIDGE_URL
    postador = Postador(ctx.bot)
    msg      = await update.effective_message.reply_text("🎯 Postando nos destinos selecionados... ⏳")
    linhas   = ["🎯 <b>Postagem Seletiva</b>\n"]

    if canais_sel:
        linhas.append("📢 <b>Telegram:</b>")
        for canal in canais_sel:
            ok, _ = await postador.postar(uid, produto, canal, copy)
            linhas.append(f"  {'✅' if ok else '❌'} <code>{canal}</code>")
            if ok:
                db.inc_postagem(uid, "telegram")
                db.log_postagem(uid, url_hash, canal, True, metodo=produto.metodo,
                                destino="telegram", url=produto.link.url_original,
                                titulo=produto.titulo, imagem=produto.imagem,
                                preco=produto.preco, desconto=produto.desconto_pct)

    if grupos_sel:
        linhas.append("\n📲 <b>WhatsApp:</b>")
        if not await wa_bridge_online(bridge):
            linhas.append("  ❌ Bridge offline")
        else:
            ok_wa, _, enviados = await postar_whatsapp(uid, produto, copy, grupos_sel, bridge)
            linhas.append(f"  {'✅' if ok_wa else '❌'} {enviados}/{len(grupos_sel)} grupo(s)")
            if ok_wa:
                db.inc_postagem(uid, "whatsapp")
                db.log_postagem(uid, url_hash, "wa_groups", True, metodo=produto.metodo,
                                destino="whatsapp", url=produto.link.url_original,
                                titulo=produto.titulo, imagem=produto.imagem,
                                preco=produto.preco, desconto=produto.desconto_pct)
    try:
        await msg.edit_text("\n".join(linhas), parse_mode="HTML",
                            reply_markup=kb(VOLTAR_MAIN[0]))
    except Exception:
        await update.effective_message.reply_text("\n".join(linhas), parse_mode="HTML",
                                                  reply_markup=kb(VOLTAR_MAIN[0]))
    
async def postar_da_biblioteca(update: Update, ctx: ContextTypes.DEFAULT_TYPE, link_id: int, force: bool = False):
    """
    force=False → respeita nicho de cada canal/grupo
    force=True  → posta em todos sem filtrar nicho
    """
    uid = update.effective_user.id
    
    # 🔥 VERIFICA ASSINATURA
    if not db.assinatura_ativa(uid):
        await reply(update, 
            "❌ <b>Assinatura inativa!</b>\n\nUse /start para renovar.",
            kb(VOLTAR_MAIN[0]), "HTML")
        return
    
    lk = db.get_link(link_id, uid)
    if not lk:
        await reply(update, "❌ Link não encontrado.", kb(VOLTAR_MAIN[0]))
        return
    
    a = db.get_assinante(uid)
    canais_ativos = [c for c in a.get("canais_tg",[]) if c in a.get("canais_tg_ativos",[])] if a else []
    grupos_ativos = [g for g in a.get("grupos_wa",[]) if g in a.get("grupos_wa_ativos",[])] if a else []
    bridge = (a or {}).get("wa_bridge_url","") or cfg.WA_BRIDGE_URL
    
    if not canais_ativos and not grupos_ativos:
        await reply(update, "⚠️ Nenhum destino ativo!",
                    kb([btn("📢 TG","menu_canais_tg")],[btn("📲 WA","menu_grupos_wa")]),"HTML")
        return
    
    msg = await update.effective_message.reply_text("⏳ Preparando...")
    
    try:
        link = LinkAnalyzer.analisar(lk["url"])
        produto = await Extratores.extrair(link)
        copy = gerar_copy(produto, a.get("estilo","padrao") if a else "padrao")
        postador = Postador(ctx.bot)

        if force:
            canais, grupos = canais_ativos, grupos_ativos
            linhas = ["🌐 <b>Biblioteca → TODOS os destinos</b>\n"]
        else:
            canais, grupos, info_nicho = _filtrar_destinos_por_nicho(
                uid, produto, canais_ativos, grupos_ativos)
            linhas = [f"✅ <b>Biblioteca</b>{info_nicho}\n"]

        for canal in canais:
            ok, _ = await postador.postar(uid, produto, canal, copy)
            linhas.append(f"  {'✅' if ok else '❌'} 📢 <code>{canal}</code>")
            if ok:
                db.inc_postagem(uid, "telegram")
                db.log_postagem(uid, link.url_hash, canal, True, metodo=produto.metodo,
                                url=lk["url"], titulo=produto.titulo, imagem=produto.imagem,
                                preco=produto.preco, desconto=produto.desconto_pct)
        
        if grupos and await wa_bridge_online(bridge):
            ok_wa, _, enviados = await postar_whatsapp(uid, produto, copy, grupos, bridge)
            linhas.append(f"  {'✅' if ok_wa else '❌'} 📲 {enviados}/{len(grupos)} grupos")
            if ok_wa:
                db.inc_postagem(uid, "whatsapp")

        await msg.edit_text("\n".join(linhas), parse_mode="HTML",
                            reply_markup=kb(VOLTAR_MAIN[0]))
    except Exception as e:
        await msg.edit_text(f"❌ Erro: {str(e)[:150]}", parse_mode="HTML")

# ══════════════════════════════════════════════════════════════
async def handle_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    uid = update.effective_user.id

    # ── Gateway de assinatura — bloqueia tudo se expirado ────────
    if not db.assinatura_ativa(uid):
        link_mp = criar_link_pagamento(uid)
        rows_kb = []
        if not db.usou_teste(uid):
            rows_kb.append([btn("🎁 7 Dias GRÁTIS", "teste_gratis")])
        rows_kb.append([btn_url("💳 Assinar R$19,99/mês", link_mp)])
        rows_kb.append([btn("✅ Já paguei — ativar agora", "verificar_pagamento")])
        rows_kb.append([btn_url("🆘 Suporte", cfg.SUPORTE_LINK)])
        await update.message.reply_text(
            "🔒 <b>Acesso bloqueado.</b>\n\n"
            "Sua assinatura está inativa ou expirou.\n\n"
            "Escolha uma opção abaixo para continuar usando o bot:",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(rows_kb))
        return

    text = update.message.text or ""
    ag = ctx.user_data.get("aguardando")
    ag_aff = ctx.user_data.get("aguardando_aff")

    # ── Cancelar ─────────────────────────────────────────────
    if text in ("❌ Cancelar", "/cancelar"):
        for k in ["aguardando","aguardando_aff","tg_req_canal","tg_req_grupo"]:
            ctx.user_data.pop(k, None)
        await update.message.reply_text("✅ Cancelado.", reply_markup=ReplyKeyboardRemove())
        await cmd_start(update, ctx)
        return

    # ── Chat compartilhado (RequestChat) ─────────────────────
    chat_shared = getattr(update.message, "chat_shared", None)
    if chat_shared and ag == "request_chat":
        ctx.user_data.pop("aguardando", None)
        await processar_chat_shared(update, ctx, chat_shared)
        return

    # ── Forward de canal ──────────────────────────────────────
    if getattr(update.message, "forward_origin", None) is not None:
        await processar_encaminhamento_tg(update, ctx)
        return

    # ── Aguardando inputs ─────────────────────────────────────
    if ag_aff:
        plataforma = ctx.user_data.pop("aguardando_aff")
        await processar_codigo_afiliado(update, ctx, plataforma, text.strip())
        return

    if ag == "canal_tg":
        ctx.user_data.pop("aguardando", None)
        await _add_canal_tg(update, ctx, text.strip())
        return

    if ag == "wa_telefone":
        ctx.user_data.pop("aguardando", None)
        await processar_wa_telefone(update, ctx, text.strip())
        return

    if ag == "email":
        ctx.user_data.pop("aguardando", None)
        await processar_email(update, ctx, text.strip())
        return

    if ag == "horario_agenda":
        ctx.user_data.pop("aguardando", None)
        await processar_horario_agenda(update, ctx, text.strip())
        return

    if ag == "horario_prod":
        ctx.user_data.pop("aguardando", None)
        await processar_horario_prod(update, ctx, text.strip())
        return

    if ag == "bl_loja":
        ctx.user_data.pop("aguardando", None)
        db.add_blacklist_loja(uid, text.strip())
        await update.message.reply_text(f"🚫 <b>Loja bloqueada:</b> {text.strip()}",
                                        parse_mode="HTML",
                                        reply_markup=kb([btn("🚫 Blacklist","menu_blacklist")]))
        return

    if ag == "bl_palavra":
        ctx.user_data.pop("aguardando", None)
        a = db.get_assinante(uid)
        bl = a.get("blacklist_produtos",[]) if a else []
        bl.append(text.strip())
        db._exec("UPDATE assinantes SET blacklist_produtos=%s WHERE id=%s",
                 (json.dumps(bl), uid))
        await update.message.reply_text(f"🚫 <b>Palavra bloqueada:</b> {text.strip()}",
                                        parse_mode="HTML",
                                        reply_markup=kb([btn("🚫 Blacklist","menu_blacklist")]))
        return

    if ag == "copy_customizada":
        ctx.user_data.pop("aguardando", None)
        url_hash = ctx.user_data.pop("hash_edit_copy", None)
        if not url_hash:
            await update.message.reply_text("⏳ Sessão expirada.")
            return
        produto = ctx.user_data.get(f"prod_{url_hash}")
        if not produto:
            await update.message.reply_text("⏳ Sessão expirada. Reenvie o link.")
            return
        # Processa variáveis na copy do usuário
        copy_usuario = text.strip()
        try:
            copy_usuario = copy_usuario.format(
                titulo=produto.titulo[:70],
                preco=produto.preco or "",
                desconto=f"{produto.desconto_pct}%" if produto.desconto_pct else "",
                loja=produto.loja or "",
                desc=_bloco_descricao(produto),
            )
        except (KeyError, ValueError):
            pass
        # Salva copy customizada no BANCO
        db.salvar_copy_custom(uid, url_hash, copy_usuario)
        ctx.user_data[f"copy_custom_{url_hash}"] = copy_usuario
        html_copy = re.sub(r"\*(.+?)\*", r"<b>\1</b>", copy_usuario)
        a = db.get_assinante(uid)
        canais_tg = [c for c in (a or {}).get("canais_tg",[])
                     if c in (a or {}).get("canais_tg_ativos",[])]
        grupos_wa = [g for g in (a or {}).get("grupos_wa",[])
                     if g in (a or {}).get("grupos_wa_ativos",[])]
        n_tg = len(canais_tg)
        n_wa = len(grupos_wa)
        rows = []
        if n_tg and n_wa:
            rows.append([btn(f"🚀 Tudo por nicho ({n_tg} TG + {n_wa} WA)", f"post_all_{url_hash}")])
            rows.append([btn(f"🌐 Postar em TODOS (ignora nicho)", f"post_all_force_{url_hash}")])
        if n_tg:
            rows.append([btn(f"📢 Telegram ({n_tg})", f"post_tg_{url_hash}"),
                         btn(f"🌐 Todos TG", f"post_tg_force_{url_hash}")])
        if n_wa:
            rows.append([btn(f"📲 WhatsApp ({n_wa})", f"post_wa_{url_hash}"),
                         btn(f"🌐 Todos WA", f"post_wa_force_{url_hash}")])
        rows.append([btn("✍️ Trocar copy", f"recopy_{url_hash}"),
                     btn("✏️ Editar texto", f"edit_copy_{url_hash}")])
        rows.append([btn("💾 Salvar", f"salvar_{url_hash}"),
                     btn("⏰ Agendar", f"agendar_prod_{url_hash}")])
        rows.append(VOLTAR_MAIN[0])
        await update.message.reply_text(
            f"✅ <b>Copy salva!</b>\n\n{html_copy}\n\n<b>Onde postar?</b>",
            parse_mode="HTML", reply_markup=InlineKeyboardMarkup(rows))
        return

    if ag == "novo_estilo_nome":
        ctx.user_data.pop("aguardando", None)
        ctx.user_data["novo_estilo_nome"] = text.strip()
        ctx.user_data["aguardando"] = "novo_estilo_texto"
        await update.message.reply_text(
            f"✅ Nome: <b>{text.strip()}</b>\n\n"
            f"2️⃣ Agora envie o <b>texto da copy</b>.\n\n"
            f"<b>Variáveis disponíveis:</b>\n"
            f"  <code>{{titulo}}</code> — nome do produto\n"
            f"  <code>{{preco}}</code> — preço\n"
            f"  <code>{{desconto}}</code> — % desconto\n"
            f"  <code>{{loja}}</code> — nome da loja\n"
            f"  <code>{{desc}}</code> — bloco completo\n"
            f"  <code>{{preco_original}}</code> — preço original\n"
            f"  <code>{{preco_original_riscado}}</code> — preço original com riscado\n\n"
            f"<i>Exemplo:</i>\n"
            f"<code>🔥 {{titulo}}\n{{preco_original_riscado}} *{{preco}}* com {{desconto}} OFF\nCorre! 👇</code>",
            parse_mode="HTML",
            reply_markup=kb([btn("❌ Cancelar","menu_estilo")]))
        return

    if ag == "novo_estilo_texto":
        ctx.user_data.pop("aguardando", None)
        nome = ctx.user_data.pop("novo_estilo_nome", "Minha Copy")
        template_texto = text.strip()
        ok = db.add_template_custom(uid, nome, template_texto)
        if ok:
            templates = db.get_templates_custom(uid)
            novo_idx = len(templates) - 1
            db.set_template_ativo(uid, novo_idx)
            await update.message.reply_text(
                f"✅ <b>Copy criada e ativada!</b>\n\n"
                f"📝 <b>{nome}</b>\n"
                f"🤖 O auto-poster vai usar esta copy agora!\n\n"
                f"Você pode trocar a qualquer momento em 🎨 Estilo de Copy.",
                parse_mode="HTML",
                reply_markup=kb([btn("🎨 Ver Estilos","menu_estilo"),
                                  btn("🏠 Menu","main_menu")]))
        else:
            await update.message.reply_text(
                "❌ Limite de templates atingido.",
                reply_markup=kb([btn("🎨 Estilos","menu_estilo")]))
        return

    if ag == "tmpl_nome":
        ctx.user_data["tmpl_nome_salvo"] = text.strip()
        ctx.user_data["aguardando"] = "tmpl_texto"
        await update.message.reply_text(
            "✏️ Agora envie o <b>texto do template</b>.\n\n"
            "Use: <code>{titulo}</code> <code>{preco}</code> <code>{loja}</code> "
            "<code>{desconto}</code> <code>{desc}</code> "
            "<code>{preco_original}</code> <code>{preco_original_riscado}</code>",
            parse_mode="HTML",
            reply_markup=kb([btn("❌ Cancelar","menu_templates")]))
        return

    if ag == "tmpl_texto":
        ctx.user_data.pop("aguardando", None)
        nome = ctx.user_data.pop("tmpl_nome_salvo", "Meu Template")
        ok = db.add_template_custom(uid, nome, text.strip())
        if ok:
            await update.message.reply_text(
                f"✅ <b>Template criado!</b>\n📝 {nome}",
                parse_mode="HTML",
                reply_markup=kb([btn("📝 Ver Templates","menu_templates")]))
        else:
            await update.message.reply_text(
                "❌ Limite de templates atingido. Faça upgrade de plano!",
                reply_markup=kb([btn("💳 Assinar","ver_planos")]))
        return

    # ── Link de produto ───────────────────────────────────────
    if re.search(r"https?://", text):
        await processar_link(update, ctx, text)
        return

    await update.message.reply_text(
        "ℹ️ Envie um <b>link de produto</b> ou use o menu! 👇",
        parse_mode="HTML", reply_markup=teclado_main(uid))
    
async def tela_cadastro_afiliado(update: Update, ctx: ContextTypes.DEFAULT_TYPE,
                                      plataforma: str = "shopee",
                                      origem: str = "link"):
    """
    Exibida quando o usuário ainda não tem o ID de afiliado cadastrado.
    origem = "link"  → veio ao enviar um produto
    origem = "auto"  → veio ao tentar ativar o auto-poster
    """
    emoji     = cfg.PLATAFORMAS.get(plataforma, {}).get("emoji", "🔗")
    plat_nome = plataforma.capitalize()
    
    guias = {
        "shopee": (
            "1️⃣ Acesse <b>affiliate.shopee.com.br</b>\n"
            "2️⃣ Faça login com sua conta Shopee\n"
            "3️⃣ Vá em <b>Ferramentas → Links de afiliado</b>\n"
            "4️⃣ Copie seu <b>ID de afiliado</b> (ex: <code>meu_id_123</code>)\n"
            "5️⃣ Cole aqui no bot 👇"
        ),
        "amazon": (
            "1️⃣ Acesse <b>associados.amazon.com.br</b>\n"
            "2️⃣ Faça login e copie sua <b>tag de associado</b>\n"
            "   Ex: <code>meutag-20</code>\n"
            "3️⃣ Cole aqui no bot 👇"
        ),
        "mercadolivre": (
            "1️⃣ Acesse <b>afiliados.mercadolivre.com.br</b>\n"
            "2️⃣ Copie seu <b>matt_tool ID</b>\n"
            "3️⃣ Cole aqui no bot 👇"
        ),
    }
    guia = guias.get(plataforma,
        f"1️⃣ Acesse o painel de afiliados da {plat_nome}\n"
        f"2️⃣ Copie seu ID/código de afiliado\n"
        f"3️⃣ Cole aqui no bot 👇"
    )
    
    if origem == "link":
        intro = (
            f"⚠️ <b>Sem ID de afiliado {plat_nome}!</b>\n\n"
            f"Sem o ID, a comissão da venda <b>não vai para você</b>. 😕\n\n"
            f"Cadastre agora — é rápido!\n\n"
            f"{emoji} <b>Como encontrar seu ID {plat_nome}:</b>\n"
            f"{guia}"
        )
        ctx.user_data["aff_origem"]  = "link"
        ctx.user_data["aff_plat_pendente"] = plataforma
    else:  # auto
        intro = (
            f"🤖 <b>Antes de ativar o Auto-poster...</b>\n\n"
            f"Você ainda não tem o ID de afiliado {plat_nome} cadastrado.\n"
            f"Sem ele, as comissões das vendas <b>não vão para você</b>. 💸\n\n"
            f"{emoji} <b>Como encontrar seu ID {plat_nome}:</b>\n"
            f"{guia}"
        )
        ctx.user_data["aff_origem"]  = "auto"
        ctx.user_data["aff_plat_pendente"] = plataforma
    
    ctx.user_data["aguardando_aff"] = plataforma
    await reply(update, intro,
        kb([btn(f"⏭️ Pular (sem comissão)", f"aff_pular_{origem}_{plataforma}")],
           [btn("❌ Cancelar", "main_menu")]),
        "HTML")
    
async def processar_link(update: Update, ctx: ContextTypes.DEFAULT_TYPE, text: str):
    uid = update.effective_user.id
    if not db.assinatura_ativa(uid):
        await update.message.reply_text(
            "❌ <b>Assinatura inativa.</b>\n\nUse /start para ativar!",
            parse_mode="HTML", reply_markup=teclado_main(uid)); return
    
    urls = re.findall(r"https?://[^\s]+", text)
    if not urls: return
    url  = urls[0]
    link = LinkAnalyzer.analisar(url)
    
    # ── Verificar se usuário tem ID de afiliado para esta plataforma ──
    if link.plataforma != "desconhecida":
        codigo = db.get_aff_code(uid, link.plataforma)
        if not codigo:
            # Salva URL para continuar depois do cadastro
            ctx.user_data["url_pendente_aff"] = url
            await tela_cadastro_afiliado(update, ctx, link.plataforma, origem="link")
            return
    
    if link.plataforma == "desconhecida":
        await update.message.reply_text(
            f"❌ Plataforma não suportada.\n"
            f"<b>Suportadas:</b> {', '.join(cfg.PLATAFORMAS.keys())}",
            parse_mode="HTML"); return
    
    emoji = cfg.PLATAFORMAS.get(link.plataforma,{}).get("emoji","🛍️")
    msg   = await update.message.reply_text(f"{emoji} <b>Analisando produto...</b> ⏳",
                                            parse_mode="HTML")
    try:
        produto = await Extratores.extrair(link)
        a       = db.get_assinante(uid)
        copy    = gerar_copy(produto, a.get("estilo","padrao") if a else "padrao")
        ctx.user_data[f"prod_{link.url_hash}"] = produto
        ctx.user_data["ultimo_url"]             = url
        html_copy = re.sub(r"\*(.+?)\*", r"<b>\1</b>", copy)
    
        preco_str = ""
        if produto.preco:
            if produto.preco_original and 5 <= produto.desconto_pct <= 80:
                preco_str = f"<s>R$ {produto.preco_original}</s>  💰 <b>R$ {produto.preco}</b>"
            else:
                preco_str = f"💰 <b>R$ {produto.preco}</b>"
            if produto.desconto_pct > 0:
                preco_str += f"  🏷️ <b>-{produto.desconto_pct}% OFF</b>"
    
        detalhes = []
        if produto.avaliacao and produto.avaliacao != "0.0":
            detalhes.append(f"⭐ {produto.avaliacao}/5")
        if produto.vendidos and produto.vendidos != "0":
            detalhes.append(f"📦 {produto.vendidos}+ vendidos")
        if produto.loja:
            detalhes.append(f"🏪 {produto.loja}")
        detalhes_str = "  |  ".join(detalhes)
    
        canais_tg = [c for c in (a or {}).get("canais_tg",[])
                     if c in (a or {}).get("canais_tg_ativos",[])]
        grupos_wa = [g for g in (a or {}).get("grupos_wa",[])
                     if g in (a or {}).get("grupos_wa_ativos",[])]
        n_tg = len(canais_tg)
        n_wa = len(grupos_wa)
    
        # Preview da mensagem final
        link_final = aplicar_afiliado(uid, produto.link.plataforma,
                                      url, produto.link_afiliado)
        aff_codigo = db.get_aff_code(uid, produto.link.plataforma)
        aff_info = f"✅ Afiliado: <code>{aff_codigo}</code>" if aff_codigo else "⚠️ Sem código afiliado"
    
        midia_str = ""
        if produto.video and produto.video.startswith("http"):
            midia_str = "🎬 <b>Vídeo disponível!</b> O post será enviado como vídeo.\n"
        elif produto.imagem:
            midia_str = "🖼️ Imagem disponível.\n"
        resumo = (
            f"{emoji} <b>Produto encontrado!</b>\n\n"
            f"🛍️ <b>{produto.titulo[:80]}</b>\n"
            f"{preco_str}\n{detalhes_str}\n\n"
            f"{midia_str}"
            f"✍️ <b>Copy:</b>\n{html_copy}\n\n"
            f"🔗 {aff_info}\n"
            f"🔧 <i>{produto.metodo}</i>\n\n"
            f"<b>Onde postar?</b>"
        )
        rows = []
        if n_tg and n_wa:
            rows.append([btn(f"🚀 Tudo por nicho ({n_tg} TG + {n_wa} WA)", f"post_all_{link.url_hash}")])
            rows.append([btn(f"🌐 Postar em TODOS (ignora nicho)", f"post_all_force_{link.url_hash}")])
        elif n_tg:
            rows.append([btn(f"📢 Por nicho ({n_tg} canais)", f"post_tg_{link.url_hash}")])
            rows.append([btn(f"🌐 Todos os canais (ignora nicho)", f"post_tg_force_{link.url_hash}")])
        elif n_wa:
            rows.append([btn(f"📲 Por nicho ({n_wa} grupos)", f"post_wa_{link.url_hash}")])
            rows.append([btn(f"🌐 Todos os grupos (ignora nicho)", f"post_wa_force_{link.url_hash}")])
        if n_tg and n_wa:
            rows.append([btn(f"📢 Só TG por nicho", f"post_tg_{link.url_hash}"),
                         btn(f"📲 Só WA por nicho", f"post_wa_{link.url_hash}")])
        rows.append([btn("🎯 Escolher Destino", f"dest_sel_{link.url_hash}")])
        rows.append([btn("✍️ Escolher Copy", f"recopy_{link.url_hash}"),
                     btn("✏️ Editar texto",  f"edit_copy_{link.url_hash}")])
        rows.append([btn("💾 Salvar",       f"salvar_{link.url_hash}"),
                     btn("⏰ Agendar",      f"agendar_prod_{link.url_hash}")])
        if not n_tg and not n_wa:
            rows.append([btn("📢 Config Telegram","menu_canais_tg")])
            rows.append([btn("📲 Config WhatsApp","menu_grupos_wa")])
        rows.append(VOLTAR_MAIN[0])
        await msg.edit_text(resumo, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(rows))
    
    except Exception as e:
        logger.error(f"Erro ao processar link: {e}")
        await msg.edit_text(f"❌ <b>Erro ao analisar.</b>\n<i>{str(e)[:150]}</i>",
                            parse_mode="HTML", reply_markup=kb(VOLTAR_MAIN[0]))
    
    
async def _add_canal_tg(update: Update, ctx: ContextTypes.DEFAULT_TYPE, canal: str):
    uid = update.effective_user.id
    canal = canal.strip()
    if not canal.startswith("@") and not canal.lstrip("-").isdigit():
        await update.message.reply_text(
            "❌ Use <code>@username</code> ou ID numérico.",
            parse_mode="HTML", reply_markup=kb([btn("🔙 Voltar","menu_canais_tg")])); return
    msg_esp = await update.message.reply_text("⏳ Verificando...")
    chat_id_final = canal
    titulo        = canal
    tipo          = "unknown"
    is_admin_bot  = False
    try:
        chat_ref  = int(canal) if canal.lstrip("-").isdigit() else canal
        chat_info = await ctx.bot.get_chat(chat_ref)
        chat_id_final = str(chat_info.id)
        titulo        = chat_info.title or chat_info.username or str(chat_info.id)
        tipo          = chat_info.type
        member        = await ctx.bot.get_chat_member(chat_info.id, ctx.bot.id)
        is_admin_bot  = member.status in ("administrator", "creator")
    except Exception:
        is_admin_bot = True  # Tenta mesmo assim
    
    if not is_admin_bot:
        await msg_esp.edit_text(
            f"❌ <b>{titulo}</b>\n\nO bot não é admin neste canal/grupo.\n\n"
            "1️⃣ Abra o canal → Administradores → Adicionar Admin\n"
            "2️⃣ Adicione o bot\n3️⃣ Tente novamente.",
            parse_mode="HTML",
            reply_markup=kb([btn("🔙 Voltar","menu_canais_tg")])); return
    
    a      = db.get_assinante(uid)
    canais = a.get("canais_tg",[]) if a else []
    ativos = a.get("canais_tg_ativos",[]) if a else []
    limite = a.get("limite_canais",10) if a else 10
    
    if chat_id_final in canais:
        await msg_esp.edit_text(f"ℹ️ <b>{titulo}</b> já está na lista!",
                                parse_mode="HTML",
                                reply_markup=kb([btn("📢 Ver Canais","menu_canais_tg")])); return
    if len(canais) >= limite:
        await msg_esp.edit_text(
            f"❌ Limite de {limite} canais atingido!\n"
            f"Faça upgrade para adicionar mais.",
            reply_markup=kb([btn("💳 Assinar","ver_planos"),
                             btn("🔙 Voltar","menu_canais_tg")])); return
    
    canais.append(chat_id_final)
    ativos.append(chat_id_final)
    db.set_canais_tg(uid, canais, ativos)
    db.registrar_bot_chat(chat_id_final, titulo, tipo)
    await msg_esp.edit_text(
        f"✅ <b>{titulo}</b> adicionado!\n🟢 Auto-post ativo.\n<i>ID: <code>{chat_id_final}</code></i>",
        parse_mode="HTML",
        reply_markup=kb([btn("📢 Ver Canais","menu_canais_tg")]))
    
    
async def processar_codigo_afiliado(update: Update, ctx: ContextTypes.DEFAULT_TYPE,
                                        plataforma: str, codigo: str):
    uid = update.effective_user.id
    codigo = codigo.strip().replace(" ","")
    if len(codigo) > 80:
        await update.message.reply_text("❌ Código muito longo.",
                                        reply_markup=kb([btn("🔙 Voltar","menu_afiliados")])); return
    db.set_aff_code(uid, plataforma, codigo)
    
    emoji = cfg.PLATAFORMAS.get(plataforma, {}).get("emoji", "🔗")
    await update.message.reply_text(
        f"✅ <b>ID de afiliado salvo!</b>\n"
        f"{emoji} {plataforma.capitalize()}: <code>{codigo}</code>\n\n"
        f"💰 Suas comissões serão creditadas automaticamente!",
        parse_mode="HTML")
    
    # ── Se veio de um link pendente, continua processando ────
    origem = ctx.user_data.pop("aff_origem", "")
    url_pendente = ctx.user_data.pop("url_pendente_aff", "")
    ctx.user_data.pop("aff_plat_pendente", None)
    
    if origem == "link" and url_pendente:
        await update.message.reply_text(
            "🔄 <b>Continuando com o produto...</b>", parse_mode="HTML")
        await processar_link(update, ctx, url_pendente)
        return
    
    if origem == "auto":
        # Ativa o auto-poster agora que tem o código
        a = db.get_assinante(uid)
        db.set_modo_auto(uid, True, a.get("min_desconto", 20) if a else 20)
        step = db.get_onboarding(uid)
        if step < 4:
            db.set_onboarding(uid, 4)
            await update.message.reply_text(
                "🎉 <b>Tudo configurado!</b>\n\n"
                "🤖 Modo automático <b>ATIVADO</b>!\n"
                "O bot vai começar a postar promoções para você!\n\n"
                "Use o menu para explorar todos os recursos 🚀",
                parse_mode="HTML",
                reply_markup=kb([btn("🏠 Ir para o Menu", "main_menu")]))
        else:
            await update.message.reply_text(
                "🤖 <b>Auto-poster ATIVADO!</b>\n"
                "Suas comissões estão garantidas. 💰",
                parse_mode="HTML",
                reply_markup=kb([btn("⚙️ Ver Auto-poster", "menu_auto")]))
        return
    
    # ── Se estiver no onboarding, avança ─────────────────────
    step = db.get_onboarding(uid)
    if step == 1:
        db.set_onboarding(uid, 2)
        await tela_onboarding(update, ctx, 2)
        return
    
    await update.message.reply_text(
        "", parse_mode="HTML",
        reply_markup=kb([btn("🔙 Afiliados", "menu_afiliados"),
                         btn("🏠 Menu", "main_menu")]))
    
    
async def processar_wa_telefone(update: Update, ctx: ContextTypes.DEFAULT_TYPE, telefone: str):
    uid = update.effective_user.id
    numero_limpo = re.sub(r"\D", "", telefone)
    
    if len(numero_limpo) < 10 or len(numero_limpo) > 15:
        await update.message.reply_text(
            "❌ Número inválido! Ex: <code>5511999998888</code>",
            parse_mode="HTML",
            reply_markup=kb([btn("🔙 Voltar","menu_grupos_wa")]))
        return
    
    # Mensagem de "gerando"
    msg = await update.message.reply_text("⏳ <b>Solicitando código...</b>", parse_mode="HTML")
    
    # Forçar o loop de eventos
    import asyncio
    try:
        result = await asyncio.wait_for(
            wa_solicitar_codigo(uid, numero_limpo),
            timeout=45
        )
    except asyncio.TimeoutError:
        await msg.edit_text(
            "❌ <b>Tempo limite excedido</b>\n\n"
            "A bridge demorou muito para responder.\n\n"
            "Verifique se está rodando:\n"
            "<code>node whats.js</code>",
            parse_mode="HTML",
            reply_markup=kb([btn("🔄 Tentar novamente", "wa_connect"),
                            btn("🔙 Voltar", "menu_grupos_wa")]))
        return
    
    logger.info(f"[WA] Resultado final: {result}")
    
    if result.get("success") and result.get("pairingCode"):
        code = result["pairingCode"]
        await msg.edit_text(
            f"🔑 <b>Código de pareamento gerado!</b>\n\n"
            f"<code>{code}</code>\n\n"
            f"📱 <b>Como usar:</b>\n"
            f"1️⃣ Abra o WhatsApp no celular\n"
            f"2️⃣ Vá em <b>Configurações → Aparelhos conectados</b>\n"
            f"3️⃣ Toque em <b>Conectar com código de pareamento</b>\n"
            f"4️⃣ Digite o código: <code>{code}</code>\n\n"
            f"⏱️ O código expira em 5 minutos",
            parse_mode="HTML",
            reply_markup=kb([btn("✅ Concluído", "menu_grupos_wa")]))
    else:
        err = result.get("error", "Erro desconhecido")
        await msg.edit_text(
            f"❌ <b>Erro ao gerar código</b>\n\n"
            f"<code>{err}</code>\n\n"
            f"🔧 <b>Diagnóstico:</b>\n"
            f"1. Bridge rodando? <code>node whats.js</code>\n"
            f"2. Porta correta? {cfg.WA_BRIDGE_URL}\n\n"
            f"🧪 <b>Teste manual:</b>\n"
            f"<code>curl -X POST {cfg.WA_BRIDGE_URL}/pairing-code -H 'Content-Type: application/json' -d '{{\"userId\": \"{uid}\", \"phoneNumber\": \"{numero_limpo}\"}}'</code>",
            parse_mode="HTML",
            reply_markup=kb([btn("🔄 Tentar novamente", "wa_connect"),
                            btn("🔙 Voltar", "menu_grupos_wa")]))
    
async def processar_chat_shared(update: Update, ctx: ContextTypes.DEFAULT_TYPE, chat_shared):
    uid    = update.effective_user.id
    cid_raw = getattr(chat_shared, "chat_id", None)
    if not cid_raw:
        await update.message.reply_text("❌ Não foi possível obter o ID.",
                                        reply_markup=ReplyKeyboardRemove()); return
    cid    = str(cid_raw)
    titulo = cid
    tipo   = "unknown"
    try:
        ci    = await ctx.bot.get_chat(cid_raw)
        titulo = ci.title or ci.username or cid
        tipo   = ci.type
    except Exception:
        pass
    is_admin_bot = False
    try:
        member       = await ctx.bot.get_chat_member(cid_raw, ctx.bot.id)
        is_admin_bot = member.status in ("administrator", "creator")
    except Exception:
        pass
    await update.message.reply_text("✅ Processando...", reply_markup=ReplyKeyboardRemove())
    if not is_admin_bot:
        await update.message.reply_text(
            f"❌ <b>{titulo}</b>\n\nBot não é admin.\n"
            "Adicione como admin e tente novamente.",
            parse_mode="HTML",
            reply_markup=kb([btn("➕ Tentar","tg_listar"),btn("🔙 Voltar","menu_canais_tg")])); return
    a      = db.get_assinante(uid)
    canais = a.get("canais_tg",[]) if a else []
    ativos = a.get("canais_tg_ativos",[]) if a else []
    limite = a.get("limite_canais",10) if a else 10
    db.registrar_bot_chat(cid, titulo, tipo)
    if cid in canais:
        await update.message.reply_text(
            f"ℹ️ <b>{titulo}</b> já está na lista!",
            parse_mode="HTML",
            reply_markup=kb([btn("📢 Ver Canais","menu_canais_tg")])); return
    if len(canais) >= limite:
        await update.message.reply_text(
            f"❌ Limite de {limite} canais. Faça upgrade!",
            reply_markup=kb([btn("💳 Assinar","ver_planos"),btn("🔙 Voltar","menu_canais_tg")])); return
    canais.append(cid)
    ativos.append(cid)
    db.set_canais_tg(uid, canais, ativos)
    emoji = "📢" if tipo == "channel" else "👥"
    await update.message.reply_text(
        f"✅ {emoji} <b>{titulo}</b> adicionado!\n🟢 Auto-post ativo.",
        parse_mode="HTML",
        reply_markup=kb([btn("➕ Adicionar outro","tg_listar"),
                        btn("📢 Ver Canais","menu_canais_tg")]))
    
    
async def processar_encaminhamento_tg(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid    = update.effective_user.id
    msg    = update.message
    origin = getattr(msg, "forward_origin", None)
    cid    = None
    title  = None
    if origin:
        for attr in ["chat", "sender_chat"]:
            obj = getattr(origin, attr, None)
            if obj:
                cid   = str(obj.id)
                title = obj.title or obj.username or cid
                break
    if not cid:
        fwd_chat = getattr(msg, "forward_from_chat", None)
        if fwd_chat:
            cid   = str(fwd_chat.id)
            title = fwd_chat.title or cid
    if not cid:
        await msg.reply_text("ℹ️ Encaminhe de um <b>canal ou grupo</b> onde o bot é admin.",
                             parse_mode="HTML",
                             reply_markup=kb([btn("📢 Ver Canais TG","menu_canais_tg")])); return
    title = title or cid
    is_admin_bot = False
    try:
        member = await ctx.bot.get_chat_member(int(cid), ctx.bot.id)
        is_admin_bot = member.status in ("administrator", "creator")
    except Exception:
        pass
    if not is_admin_bot:
        await msg.reply_text(f"❌ <b>{title}</b> – bot não é admin.",
                             parse_mode="HTML",
                             reply_markup=kb([btn("📢 Ver Canais","menu_canais_tg")])); return
    a      = db.get_assinante(uid)
    canais = a.get("canais_tg",[]) if a else []
    ativos = a.get("canais_tg_ativos",[]) if a else []
    if cid in canais:
        await msg.reply_text(f"ℹ️ <b>{title}</b> já na lista!",
                             parse_mode="HTML",
                             reply_markup=kb([btn("📢 Ver Canais","menu_canais_tg")])); return
    canais.append(cid)
    ativos.append(cid)
    db.set_canais_tg(uid, canais, ativos)
    await msg.reply_text(f"✅ <b>{title}</b> adicionado!\n🟢 Auto-post ativo.",
                         parse_mode="HTML",
                         reply_markup=kb([btn("📢 Ver Canais","menu_canais_tg")]))
    
    
async def processar_email(update: Update, ctx: ContextTypes.DEFAULT_TYPE, email: str):
    uid = update.effective_user.id
    if not re.match(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$", email):
        await update.message.reply_text(
            "❌ Email inválido!", reply_markup=kb([btn("❌ Cancelar","menu_config")])); return
    db.salvar_email(email, uid)
    await Notif.novo_email(uid, email, update.effective_user.username)
    await update.message.reply_text(f"✅ <b>Email salvo!</b>\n<code>{email}</code>",
                                    parse_mode="HTML", reply_markup=kb(VOLTAR_MAIN[0]))
    
    
async def processar_horario_agenda(update: Update, ctx: ContextTypes.DEFAULT_TYPE, horario: str):
    uid     = update.effective_user.id
    link_id = ctx.user_data.pop("agendar_link_id", None)
    if not re.match(r"^([01]?\d|2[0-3]):[0-5]\d$", horario):
        await update.message.reply_text("❌ Use <code>HH:MM</code>", parse_mode="HTML"); return
    if horario not in cfg.HORARIOS_POSTAGEM:
        hors = "  ".join(f"<code>{h}</code>" for h in cfg.HORARIOS_POSTAGEM)
        await update.message.reply_text(f"❌ Disponíveis:\n{hors}", parse_mode="HTML"); return
    lk = db.get_link(link_id, uid) if link_id else None
    if not lk:
        await update.message.reply_text("❌ Link não encontrado."); return
    a = db.get_assinante(uid)
    canal    = a["canais_tg"][0] if a and a.get("canais_tg") else "sem_canal"
    destinos = ctx.user_data.pop("destinos_agenda", "telegram")
    db.agendar(uid, lk["id"], lk["url"], lk["url_hash"],
               lk.get("titulo","Produto"), canal, horario, destinos)
    await update.message.reply_text(f"✅ <b>Agendado para {horario}!</b>",
                                    parse_mode="HTML",
                                    reply_markup=kb([btn("⏰ Ver Agenda","menu_agenda")],
                                                   VOLTAR_MAIN[0]))
    
    
async def processar_horario_prod(update: Update, ctx: ContextTypes.DEFAULT_TYPE, horario: str):
    uid      = update.effective_user.id
    url_hash = ctx.user_data.pop("hash_produto", None)
    produto  = ctx.user_data.get(f"prod_{url_hash}") if url_hash else None
    url      = ctx.user_data.get("ultimo_url")

    if not re.match(r"^([01]?\d|2[0-3]):[0-5]\d$", horario):
        await update.message.reply_text("❌ Use <code>HH:MM</code>", parse_mode="HTML"); return
    if horario not in cfg.HORARIOS_POSTAGEM:
        hors = "  ".join(f"<code>{h}</code>" for h in cfg.HORARIOS_POSTAGEM)
        await update.message.reply_text(f"❌ Disponíveis:\n{hors}", parse_mode="HTML"); return
    if not produto or not url:
        await update.message.reply_text("❌ Sessão expirada. Envie o link novamente."); return

    a        = db.get_assinante(uid)
    canal    = a["canais_tg"][0] if a and a.get("canais_tg") else "sem_canal"
    destinos = ctx.user_data.pop("destinos_agenda", "telegram")

    # Usa o link com afiliado aplicado como URL para postar
    url_post = aplicar_afiliado(
        uid, produto.link.plataforma,
        produto.link.url_original,
        produto.link_afiliado)

    # Salva copy customizada se existir
    copy_custom = (db.get_copy_custom(uid, url_hash) or
                   ctx.user_data.get(f"copy_custom_{url_hash}"))
    if copy_custom:
        db.salvar_copy_custom(uid, url_hash, copy_custom)

    lid = db.salvar_link(uid, url_post, produto.titulo, produto.link.plataforma,
                         url_hash, produto.preco, produto.imagem)
    db.agendar(uid, lid, url_post, url_hash, produto.titulo, canal, horario, destinos)

    await update.message.reply_text(
        f"✅ <b>Agendado para {horario}!</b>\n\n"
        f"📦 {produto.titulo[:40]}\n"
        f"🔗 Link com afiliado salvo.",
        parse_mode="HTML",
        reply_markup=kb([btn("⏰ Ver Agenda", "menu_agenda")], VOLTAR_MAIN[0]))
    
    
    
async def tela_config_canal(update: Update, ctx: ContextTypes.DEFAULT_TYPE,
                                   destino_id: str, tipo: str = "tg"):
    """Tela principal de configuração de um canal/grupo — nicho + copy."""
    uid       = update.effective_user.id
    nichos    = db.get_nichos_tg(uid) if tipo == "tg" else db.get_nichos_wa(uid)
    nicho_atual = nichos.get(destino_id, "todos")
    templates_dest = db.get_templates_tg(uid) if tipo == "tg" else db.get_templates_wa(uid)
    tmpl_idx  = templates_dest.get(destino_id, -1)
    all_tmpls = db.get_templates_custom(uid)
    tmpl_ativo_global = db.get_template_ativo(uid)
    a         = db.get_assinante(uid)
    estilo_global = a.get("estilo","padrao") if a else "padrao"
    chats_db  = {c["chat_id"]: c["titulo"] for c in db.listar_bot_chats()}
    nomes_wa  = db.get_nomes_grupos_wa(uid)
    
    if tipo == "tg":
        nome = chats_db.get(destino_id) or destino_id[:25]
    else:
        nome = nomes_wa.get(destino_id) or _nome_curto_grupo(destino_id)
    
    # Determina copy ativa para este canal
    if tmpl_idx >= 0 and 0 <= tmpl_idx < len(all_tmpls):
        copy_nome = f"📝 {all_tmpls[tmpl_idx]['nome']}"
    elif tmpl_ativo_global >= 0 and 0 <= tmpl_ativo_global < len(all_tmpls):
        copy_nome = f"📝 {all_tmpls[tmpl_ativo_global]['nome']} (global)"
    else:
        copy_nome = ESTILOS.get(estilo_global,{}).get("nome","Padrão") + " (global)"
    
    texto = (
        f"⚙️ <b>Configurar — {nome}</b>\n\n"
        f"🌐 Nicho: <b>{CATEGORIAS_AUTO.get(nicho_atual,'Todos')}</b>\n"
        f"✍️ Copy: <b>{copy_nome}</b>\n\n"
        f"Configure o que este canal vai postar:"
    )
    voltar = "menu_canais_tg" if tipo == "tg" else "menu_grupos_wa"
    rows = [
        [btn(f"🌐 Mudar Nicho",  f"nicho_menu_{tipo}_{destino_id}")],
        [btn(f"✍️ Mudar Copy",   f"copy_menu_{tipo}_{destino_id}")],
        [btn("🔙 Voltar", voltar)],
    ]
    await reply(update, texto, InlineKeyboardMarkup(rows), "HTML")
    
    
async def tela_nicho_canal(update: Update, ctx: ContextTypes.DEFAULT_TYPE,
                                destino_id: str, tipo: str = "tg"):
    """Tela para escolher o nicho."""
    uid     = update.effective_user.id
    nichos  = db.get_nichos_tg(uid) if tipo == "tg" else db.get_nichos_wa(uid)
    atual   = nichos.get(destino_id, "todos")
    chats_db = {c["chat_id"]: c["titulo"] for c in db.listar_bot_chats()}
    nomes_wa = db.get_nomes_grupos_wa(uid)
    nome = chats_db.get(destino_id) or destino_id[:25] if tipo == "tg"            else nomes_wa.get(destino_id) or _nome_curto_grupo(destino_id)
    
    texto = (f"🌐 <b>Nicho — {nome}</b>\n\n"
             f"Atual: <b>{CATEGORIAS_AUTO.get(atual,'Todos')}</b>\n\n"
             f"Escolha o nicho dos produtos:")
    rows = []
    for k, v in CATEGORIAS_AUTO.items():
        check = "✅ " if k == atual else ""
        rows.append([btn(f"{check}{v}", f"set_nicho_{tipo}_{destino_id}_{k}")])
    rows.append([btn("🔙 Voltar", f"cfg_canal_{tipo}_{destino_id}")])
    await reply(update, texto, InlineKeyboardMarkup(rows), "HTML")
    
    
async def tela_copy_canal(update: Update, ctx: ContextTypes.DEFAULT_TYPE,
                               destino_id: str, tipo: str = "tg"):
    """Tela para escolher a copy de um canal/grupo específico."""
    uid       = update.effective_user.id
    templates_dest = db.get_templates_tg(uid) if tipo == "tg" else db.get_templates_wa(uid)
    tmpl_idx  = templates_dest.get(destino_id, -1)
    all_tmpls = db.get_templates_custom(uid)
    a         = db.get_assinante(uid)
    estilo_g  = a.get("estilo","padrao") if a else "padrao"
    tmpl_g    = db.get_template_ativo(uid)
    chats_db  = {c["chat_id"]: c["titulo"] for c in db.listar_bot_chats()}
    nomes_wa  = db.get_nomes_grupos_wa(uid)
    nome = chats_db.get(destino_id) or destino_id[:20] if tipo == "tg"            else nomes_wa.get(destino_id) or _nome_curto_grupo(destino_id)
    
    # Copy global ativa
    if tmpl_g >= 0 and 0 <= tmpl_g < len(all_tmpls):
        global_nome = f"📝 {all_tmpls[tmpl_g]['nome']}"
    else:
        global_nome = ESTILOS.get(estilo_g,{}).get("nome","Padrão")
    
    texto = (
        f"✍️ <b>Copy — {nome}</b>\n\n"
        f"Escolha a copy para este canal/grupo.\n"
        f"Sobrescreve o estilo global para este destino.\n\n"
        f"Global atual: <b>{global_nome}</b>"
    )
    rows = []
    # Opção: usar global
    check_global = "✅ " if tmpl_idx < 0 else ""
    rows.append([btn(f"{check_global}🌐 Usar global ({global_nome[:20]})",
                     f"set_copy_canal_{tipo}_{destino_id}_-1")])
    
    # Estilos padrão
    rows.append([btn("── Estilos Automáticos ──", "noop")])
    for k, v in ESTILOS.items():
        rows.append([btn(v["nome"], f"set_copy_estilo_{tipo}_{destino_id}_{k}")])
    
    # Templates personalizados
    if all_tmpls:
        rows.append([btn("── Suas Copies ──", "noop")])
        for i, t in enumerate(all_tmpls):
            check = "✅ " if tmpl_idx == i else "📝 "
            rows.append([btn(f"{check}{t['nome'][:30]}",
                             f"set_copy_canal_{tipo}_{destino_id}_{i}")])
    
    rows.append([btn("🔙 Voltar", f"cfg_canal_{tipo}_{destino_id}")])
    await reply(update, texto, InlineKeyboardMarkup(rows), "HTML")
    
async def tela_listar_canais_tg(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    import hashlib as _hl
    req_id_canal = int(_hl.md5(f"canal_{uid}".encode()).hexdigest()[:7], 16) % 2147483647 or 1
    req_id_grupo = int(_hl.md5(f"grupo_{uid}".encode()).hexdigest()[:7], 16) % 2147483647 or 2
    btn_canal = KeyboardButton(
        text="📢 Selecionar Canal",
        request_chat=KeyboardButtonRequestChat(
            request_id=req_id_canal, chat_is_channel=True,
            user_administrator_rights=ChatAdministratorRights(
                can_post_messages=True, is_anonymous=False, can_manage_chat=False,
                can_delete_messages=False, can_manage_video_chats=False,
                can_restrict_members=False, can_promote_members=False,
                can_change_info=False, can_invite_users=False,
                can_post_stories=False, can_edit_stories=False, can_delete_stories=False),
            bot_administrator_rights=ChatAdministratorRights(
                can_post_messages=True, is_anonymous=False, can_manage_chat=True,
                can_delete_messages=False, can_manage_video_chats=False,
                can_restrict_members=False, can_promote_members=False,
                can_change_info=False, can_invite_users=False,
                can_post_stories=False, can_edit_stories=False, can_delete_stories=False),
            bot_is_member=True))
    btn_grupo = KeyboardButton(
        text="👥 Selecionar Grupo",
        request_chat=KeyboardButtonRequestChat(
            request_id=req_id_grupo, chat_is_channel=False,
            user_administrator_rights=ChatAdministratorRights(
                can_post_messages=False, is_anonymous=False, can_manage_chat=True,
                can_delete_messages=False, can_manage_video_chats=False,
                can_restrict_members=False, can_promote_members=False,
                can_change_info=False, can_invite_users=False,
                can_post_stories=False, can_edit_stories=False, can_delete_stories=False),
            bot_administrator_rights=ChatAdministratorRights(
                can_post_messages=False, is_anonymous=False, can_manage_chat=True,
                can_delete_messages=False, can_manage_video_chats=False,
                can_restrict_members=False, can_promote_members=False,
                can_change_info=False, can_invite_users=False,
                can_post_stories=False, can_edit_stories=False, can_delete_stories=False),
            bot_is_member=True))
    ctx.user_data["tg_req_canal"] = req_id_canal
    ctx.user_data["tg_req_grupo"] = req_id_grupo
    ctx.user_data["aguardando"]   = "request_chat"
    await update.effective_message.reply_text(
        "📢 <b>Adicionar Canal ou Grupo</b>\n\n"
        "Clique no botão e selecione na sua lista:",
        parse_mode="HTML",
        reply_markup=ReplyKeyboardMarkup(
            [[btn_canal, btn_grupo], [KeyboardButton("❌ Cancelar")]],
            resize_keyboard=True, one_time_keyboard=True))
    
    
async def tela_listar_grupos_wa(update: Update, ctx: ContextTypes.DEFAULT_TYPE,
                                        pagina: int = 0):
    uid = update.effective_user.id
    
    # Sempre rebusca na página 0 para garantir grupos novos
    grupos_cache = ctx.user_data.get("wa_grupos_cache")
    if not grupos_cache or pagina == 0:
        ctx.user_data.pop("wa_grupos_cache", None)  # limpa cache antigo
        try:
            loading_msg = await update.effective_message.reply_text("⏳ Buscando grupos...")
        except Exception:
            loading_msg = None
        grupos_api = await wa_listar_grupos(uid)
        if not grupos_api:
            txt  = "❌ <b>Nenhum grupo encontrado.</b>\n\nWhatsApp conectado e em algum grupo?"
            rows = [[btn("🔙 Voltar","menu_grupos_wa")]]
            try:
                await loading_msg.edit_text(txt, parse_mode="HTML",
                                            reply_markup=InlineKeyboardMarkup(rows))
            except Exception:
                await update.effective_message.reply_text(txt, parse_mode="HTML",
                                                           reply_markup=InlineKeyboardMarkup(rows))
            return
        # Salva no cache do usuário para paginação sem rebuscar
        ctx.user_data["wa_grupos_cache"] = grupos_api
        grupos_cache = grupos_api
    
        # Atualiza nomes no banco
        a           = db.get_assinante(uid)
        nomes_cache = db.get_nomes_grupos_wa(uid)
        for g in grupos_api:
            gid  = g.get("id","")
            nome = g.get("nome","")
            if gid and nome:
                nomes_cache[gid] = nome
        if a:
            db.set_grupos_wa(uid,
                             a.get("grupos_wa",[]),
                             a.get("grupos_wa_ativos",[]),
                             nomes_cache)
    else:
        loading_msg = None
    
    a              = db.get_assinante(uid)
    ja_adicionados = a.get("grupos_wa",[]) if a else []
    ja_ativos      = a.get("grupos_wa_ativos",[]) if a else []
    
    # Paginação — 10 grupos por página
    por_pag   = 10
    total     = len(grupos_cache)
    tot_pags  = max(1, (total + por_pag - 1) // por_pag)
    inicio    = pagina * por_pag
    fim       = inicio + por_pag
    pag_atual = grupos_cache[inicio:fim]
    
    texto = (f"📋 <b>Seus Grupos WA</b>  "
             f"({total} grupos · pág {pagina+1}/{tot_pags})\n\n"
             f"<i>➕ = adicionar  🟢 = ativo  🔴 = pausado</i>\n\n")
    rows  = []
    
    for g in pag_atual:
        gid      = g.get("id","")
        nome     = g.get("nome") or _nome_curto_grupo(gid)
        part     = g.get("participantes",0)
        part_str = f" · {part}👥" if part else ""
        texto   += f"• <b>{nome}</b>{part_str}\n"
        if gid in ja_adicionados:
            st = "🟢" if gid in ja_ativos else "🔴"
            rows.append([btn(f"{st} {nome[:30]}", f"wa_toggle_{gid}"),
                          btn("🗑️", f"wa_confirm_del_{gid}")])
        else:
            rows.append([btn(f"➕ {nome[:35]}", f"wa_add_id_{gid}")])
    
    # Navegação — sempre mostra página atual e total
    nav = []
    if pagina > 0:
        nav.append(btn(f"◀️ Pág {pagina}", f"wa_lista_pag_{pagina-1}"))
    if tot_pags > 1:
        nav.append(btn(f"📄 {pagina+1}/{tot_pags}", "noop"))
    if pagina < tot_pags - 1:
        nav.append(btn(f"Pág {pagina+2} ▶️", f"wa_lista_pag_{pagina+1}"))
    if nav:
        rows.append(nav)
    
    rows.append([btn("🔄 Atualizar lista", "wa_listar"),
                 btn("🔙 Voltar",          "menu_grupos_wa")])
    
    markup = InlineKeyboardMarkup(rows)
    if loading_msg:
        try:
            await loading_msg.edit_text(texto, parse_mode="HTML", reply_markup=markup)
            return
        except Exception:
            pass
    try:
        await update.effective_message.edit_text(texto, parse_mode="HTML", reply_markup=markup)
    except Exception:
        await update.effective_message.reply_text(texto, parse_mode="HTML", reply_markup=markup)
    
    
    # ══════════════════════════════════════════════════════════════
    #  HANDLER /start e /status
    # ══════════════════════════════════════════════════════════════
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    nome = update.effective_user.first_name or "usuário"
    args = ctx.args or []

    if args and args[0].startswith("ref_"):
        try:
            ref_id = int(args[0][4:])
            if ref_id != uid:
                db.salvar_referral(ref_id, uid)
                await update.effective_message.reply_text(
                    "🎉 Você foi convidado! Ao assinar, <b>ambos ganham dias extras</b>! 🎁",
                    parse_mode="HTML")
        except (ValueError, TypeError):
            pass

    # 🔥 SINCRONIZAR NOVO USUÁRIO PARA O NEON (silencioso)
    if not db.usou_teste(uid):
        try:
            vencimento = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d")
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    'https://botautomacao.vercel.app/sincronizar/usuario',
                    json={
                        'id': uid,
                        'vencimento': vencimento,
                        'plano': 'teste',
                        'nome': nome
                    },
                    timeout=5
                ) as resp:
                    if resp.status == 200:
                        print(f"✅ Usuário {uid} cadastrado no Neon (teste 7 dias)")
        except Exception as e:
            print(f"❌ Erro ao cadastrar {uid} no Neon: {e}")

    # 🔥 VERIFICA ASSINATURA NO NEON (silencioso)
    ativo = db.assinatura_ativa(uid)

    if not ativo:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"https://botautomacao.vercel.app/verificar/{uid}", timeout=5) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get('ativo'):
                            vencimento = data.get('vencimento', '')
                            if vencimento and 'T' in vencimento:
                                vencimento = vencimento.split('T')[0]
                            db.ativar(uid, 30, data.get('plano', 'mensal'))
                            ativo = True
        except Exception as e:
            print(f"Erro ao consultar Neon: {e}")

    if ativo:
        a = db.get_assinante(uid)
        venc = Database._parse_date(a["vencimento"])
        dias_rest = (datetime.strptime(venc,"%Y-%m-%d") - datetime.now()).days
        
        # 🔥 VERIFICAR SE EXPIROU
        if dias_rest <= 0:
            # Desativa o usuário
            db.desativar(uid)
            
            # Mostra tela de pagamento
            texto = (
                f"⏰ <b>Sua assinatura expirou!</b>\n\n"
                f"👋 Olá, <b>{nome}</b>!\n\n"
                f"💳 <b>Renove agora para continuar:</b>\n\n"
                f"┣ ✅ Acesso a todos os recursos\n"
                f"┣ 🤖 Auto-poster ilimitado\n"
                f"┣ 📲 Postagens no WhatsApp\n"
                f"┣ 📢 Canais do Telegram\n"
                f"┗ 🎁 Links de afiliado automáticos\n\n"
                f"💰 <b>Planos a partir de R$ 19,90/mês</b>\n\n"
                f"<i>Escolha seu plano abaixo:</i>"
            )
            await update.effective_message.reply_text(
                texto, 
                parse_mode="HTML", 
                reply_markup=teclado_planos(uid)  # Teclado com opções de pagamento
            )
            return

        # Verifica onboarding
        step = db.get_onboarding(uid)
        if step < 4:
            await tela_onboarding(update, ctx, step)
            return

        stats = db.get_stats(uid)
        canais_tg = a.get("canais_tg",[])
        ativos_tg = a.get("canais_tg_ativos",[])
        grupos_wa = a.get("grupos_wa",[])
        ativos_wa = a.get("grupos_wa_ativos",[])
        modo_str = "🤖 Automático ATIVO" if a.get("modo_auto") else "👤 Manual"
        plano_nm = PLANOS.get(a.get("plano","mensal"),{}).get("nome","Mensal")

        # Alerta de vencimento próximo
        aviso = ""
        if dias_rest <= 3:
            aviso = f"\n⚠️ <b>Assinatura vence em {dias_rest} dia(s)!</b>\n"

        texto = (
            f"👋 Olá, <b>{nome}</b>!{aviso}\n\n"
            f"✅ <b>{plano_nm}</b> – {dias_rest} dias restantes\n"
            f"📅 Vence: <code>{venc}</code>\n\n"
            f"📢 TG: <b>{len(ativos_tg)}/{len(canais_tg)}</b>  "
            f"📲 WA: <b>{len(ativos_wa)}/{len(grupos_wa)}</b>\n"
            f"📬 Posts: <b>{stats.get('total_postagens',0)}</b> TG  "
            f"<b>{stats.get('total_wa',0)}</b> WA\n"
            f"📈 Esta semana: <b>{stats.get('total_semana',0)}</b>\n"
            f"🤖 Modo: <b>{modo_str}</b>\n\n"
            f"<i>Envie um link para começar! 🚀</i>"
        )
    else:
        texto = (
            f"👋 Olá, <b>{nome}</b>! Bem-vindo ao <b>Bot Afiliados PRO</b>! 🤖\n\n"
            f"🚀 <b>O que faço:</b>\n"
            f"┣ 🛍️ Extraio dados reais dos produtos\n"
            f"┣ ✍️ Gero copy persuasiva automaticamente\n"
            f"┣ 📢 Posto em canais/grupos Telegram\n"
            f"┣ 📲 Posto em grupos WhatsApp\n"
            f"┣ 🤖 Auto-poster inteligente e automático\n"
            f"┣ ⏰ Agendamento de postagens\n"
            f"┗ 🔗 Seus links de afiliado em cada post!\n\n"
            f"<b>Plataformas:</b> Shopee • Amazon • Magalu • AliExpress • ML • Hotmart\n\n"
        )
        if not db.usou_teste(uid):
            texto += "🎁 <b>Comece GRÁTIS – 7 dias com acesso completo!</b>"
        else:
            texto += "💳 <b>Assine por R$19,99/mês e tenha acesso a tudo!</b>"

    await update.effective_message.reply_text(
        texto, parse_mode="HTML", reply_markup=teclado_main(uid))
    
async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    
    # 🔥 VERIFICA ASSINATURA NO NEON SE NÃO ESTIVER ATIVA NO SQLITE
    ativo = db.assinatura_ativa(uid)
    
    if not ativo:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"https://botautomacao.vercel.app/verificar/{uid}", timeout=5) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get('ativo'):
                            # Extrai a data do vencimento
                            vencimento = data.get('vencimento', '')
                            if vencimento and 'T' in vencimento:
                                vencimento = vencimento.split('T')[0]
                            
                            # Ativa no SQLite local
                            db.ativar(uid, 30, data.get('plano', 'mensal'))
                            ativo = True
        except Exception as e:
            print(f"Erro ao consultar Neon no status: {e}")
    
    if not ativo:
        await update.message.reply_text("❌ Sem assinatura ativa. Use /start.", parse_mode="HTML")
        return
    
    a = db.get_assinante(uid)
    venc = Database._parse_date(a["vencimento"])
    dias_rest = (datetime.strptime(venc,"%Y-%m-%d") - datetime.now()).days
    stats = db.get_stats(uid)
    modo = "🟢 AUTO ON" if a.get("modo_auto") else "🔴 Manual"
    canais_tg = a.get("canais_tg",[])
    ativos_tg = a.get("canais_tg_ativos",[])
    grupos_wa = a.get("grupos_wa",[])
    ativos_wa = a.get("grupos_wa_ativos",[])
    plano_nm = PLANOS.get(a.get("plano","mensal"),{}).get("nome","Mensal")
    ultimo = a.get("ultimo_auto_post","")
    proximo = "agora"
    
    if ultimo:
        try:
            diff = (datetime.now() - datetime.fromisoformat(ultimo)).total_seconds()
            mins = max(0, cfg.AUTO_POSTER_INTERVALO - int(diff/60))
            proximo = f"{mins}min" if mins > 0 else "agora"
        except Exception:
            pass
    
    await update.message.reply_text(
        f"⚡ <b>Status Rápido</b>\n\n"
        f"📋 {plano_nm}  |  📅 {dias_rest}d restantes\n"
        f"📢 TG: {len(ativos_tg)}/{len(canais_tg)}  📲 WA: {len(ativos_wa)}/{len(grupos_wa)}\n"
        f"📬 Posts: {stats.get('total_postagens',0)} TG  {stats.get('total_wa',0)} WA\n"
        f"🤖 {modo}  ⏩ Próximo: {proximo}",
        parse_mode="HTML")
    
    # ══════════════════════════════════════════════════════════════
    #  CALLBACK QUERY HANDLER
    # ══════════════════════════════════════════════════════════════
async def callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q    = update.callback_query
    uid  = q.from_user.id
    data = q.data
    # Responde o callback imediatamente para não travar o Telegram
    try:
        await q.answer()
    except Exception:
        pass  # callback expirado — continua normalmente

    # ── Gateway de assinatura — callbacks liberados sem assinatura ──
    CALLBACKS_LIVRES = {
        "main_menu", "ver_planos", "teste_gratis",
        "verificar_pagamento", "onboard_start",
    }
    if data not in CALLBACKS_LIVRES and not data.startswith("onboard_"):
        if not db.assinatura_ativa(uid):
            link_mp = criar_link_pagamento(uid)
            rows_kb = []
            if not db.usou_teste(uid):
                rows_kb.append([btn("🎁 7 Dias GRÁTIS", "teste_gratis")])
            rows_kb.append([btn_url("💳 Assinar R$19,99/mês", link_mp)])
            rows_kb.append([btn("✅ Já paguei — ativar agora", "verificar_pagamento")])
            try:
                await q.message.reply_text(
                    "🔒 <b>Acesso bloqueado.</b>\n\n"
                    "Sua assinatura expirou. Renove para continuar:",
                    parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup(rows_kb))
            except Exception:
                pass
            return
    
    if data == "main_menu":
        await cmd_start(update, ctx)

    elif data == "verificar_pagamento":
        await cmd_pagou(update, ctx)
    
    # ── Planos / Assinatura ───────────────────────────────────
    elif data == "ver_planos":
        await tela_ver_planos(update, ctx)
    elif data == "teste_gratis":
        if db.usou_teste(uid):
            await reply(update, "❌ Você já usou o teste.", kb(VOLTAR_MAIN[0]))
        else:
            venc = db.ativar_teste(uid)
            await reply(update,
                f"🎉 <b>Teste ativado!</b>\n📅 Válido até: <b>{venc}</b>\n\n"
                "✅ 7 dias com acesso completo!\n\nVamos configurar tudo? 👇",
                kb([btn("🚀 Configurar Agora","onboard_start")],
                   VOLTAR_MAIN[0]), "HTML")
            await Notif.nova_assinatura(uid,"teste",venc,metodo="teste_gratis")
    
    # ── Onboarding ────────────────────────────────────────────
    elif data == "onboard_start":
        db.set_onboarding(uid, 0)
        await tela_onboarding(update, ctx, 0)
    elif data.startswith("onboard_skip_"):
        step = int(data[13:])
        db.set_onboarding(uid, step)
        if step >= 4:
            await cmd_start(update, ctx)
        else:
            await tela_onboarding(update, ctx, step)
    elif data.startswith("onboard_estilo_"):
        estilo = data[15:]
        db.set_estilo(uid, estilo)
        db.set_onboarding(uid, 3)
        await q.answer(f"✅ {ESTILOS.get(estilo,{}).get('nome','OK')} ativado!")
        await tela_onboarding(update, ctx, 3)
    elif data == "onboard_auto_on":
        codigo_shopee = db.get_aff_code(uid, "shopee")
        if not codigo_shopee:
            await q.answer("⚠️ Configure seu ID Shopee primeiro!")
            await tela_cadastro_afiliado(update, ctx, "shopee", origem="auto")
        else:
            db.set_modo_auto(uid, True, 20)
            db.set_onboarding(uid, 4)
            await reply(update,
                "🎉 <b>Tudo configurado!</b>\n\n"
                "🤖 Modo automático <b>ATIVADO</b>!\n"
                "O bot vai começar a postar promoções para você!\n\n"
                "Use o menu para explorar todos os recursos 🚀",
                kb([btn("🏠 Ir para o Menu","main_menu")]), "HTML")
    
    # ── Canais Telegram ───────────────────────────────────────
    elif data == "menu_canais_tg":
        await tela_canais_tg(update, ctx)
    elif data == "tg_listar":
        await tela_listar_canais_tg(update, ctx)
    elif data.startswith("tg_add_id_"):
        cid    = data[10:]
        a      = db.get_assinante(uid)
        canais = a.get("canais_tg",[]) if a else []
        ativos = a.get("canais_tg_ativos",[]) if a else []
        limite = a.get("limite_canais",10) if a else 10
        if cid not in canais and len(canais) < limite:
            canais.append(cid); ativos.append(cid)
            db.set_canais_tg(uid, canais, ativos)
            await q.answer("✅ Adicionado!")
        else:
            await q.answer("⚠️ Já na lista ou limite atingido")
        await tela_listar_canais_tg(update, ctx)
    elif data.startswith("cfg_canal_tg_"):
        await tela_config_canal(update, ctx, data[13:], "tg")
    elif data.startswith("cfg_canal_wa_"):
        await tela_config_canal(update, ctx, data[13:], "wa")
    elif data.startswith("nicho_menu_tg_"):
        await tela_nicho_canal(update, ctx, data[14:], "tg")
    elif data.startswith("nicho_menu_wa_"):
        await tela_nicho_canal(update, ctx, data[14:], "wa")
    elif data.startswith("copy_menu_tg_"):
        await tela_copy_canal(update, ctx, data[13:], "tg")
    elif data.startswith("copy_menu_wa_"):
        await tela_copy_canal(update, ctx, data[13:], "wa")
    elif data.startswith("set_copy_canal_"):
        # set_copy_canal_{tipo}_{destino_id}_{idx}
        partes = data[15:].split("_")
        tipo   = partes[0]
        # idx é o último elemento, destino é o resto
        idx_str  = partes[-1]
        dest_id  = "_".join(partes[1:-1])
        idx = int(idx_str)
        if tipo == "tg": db.set_template_canal_tg(uid, dest_id, idx)
        else: db.set_template_grupo_wa(uid, dest_id, idx)
        if idx >= 0: db.set_template_ativo(uid, idx)
        nome_copy = "Global" if idx < 0 else (db.get_templates_custom(uid)[idx]["nome"] if 0 <= idx < len(db.get_templates_custom(uid)) else "?")
        await q.answer(f"✅ Copy: {nome_copy}")
        await tela_config_canal(update, ctx, dest_id, tipo)
    elif data.startswith("set_copy_estilo_"):
        # set_copy_estilo_{tipo}_{destino_id}_{estilo}
        resto = data[16:]
        estilo_key = None
        for k in ESTILOS:
            if resto.endswith("_" + k):
                estilo_key = k
                resto = resto[:-(len(k)+1)]
                break
        if estilo_key:
            tipo, dest_id = resto.split("_", 1)
            # Salva como estilo no banco do usuário para este canal
            # Por ora define o estilo global (simplificado)
            db.set_estilo(uid, estilo_key)
            if tipo == "tg": db.set_template_canal_tg(uid, dest_id, -1)
            else: db.set_template_grupo_wa(uid, dest_id, -1)
            await q.answer(f"✅ {ESTILOS[estilo_key]['nome']}")
            await tela_config_canal(update, ctx, dest_id, tipo)
    elif data.startswith("set_nicho_tg_"):
        partes  = data[13:].rsplit("_", 1)
        canal, nicho = partes[0], partes[1]
        db.set_nicho_tg(uid, canal, nicho)
        await q.answer(f"✅ {CATEGORIAS_AUTO.get(nicho,'OK')}")
        await tela_nicho_canal(update, ctx, canal, "tg")
    elif data.startswith("set_nicho_wa_"):
        partes  = data[13:].rsplit("_", 1)
        grupo, nicho = partes[0], partes[1]
        db.set_nicho_wa(uid, grupo, nicho)
        await q.answer(f"✅ {CATEGORIAS_AUTO.get(nicho,'OK')}")
        await tela_nicho_canal(update, ctx, grupo, "wa")
    elif data.startswith("tg_toggle_"):
        canal = data[10:]
        novo  = db.toggle_canal_tg(uid, canal)
        await q.answer(f"{'🟢 Ativado' if novo else '🔴 Pausado'}")
        try:
            await tela_canais_tg(update, ctx)
        except Exception:
            pass
    elif data.startswith("tg_confirm_del_"):
        canal = data[15:]
        chats_db = {c["chat_id"]: c["titulo"] for c in db.listar_bot_chats()}
        nome = chats_db.get(canal) or canal[:20]
        await reply(update,
            f"🗑️ <b>Remover canal?</b>\n\n<b>{nome}</b>",
            kb([btn("✅ Sim, remover", f"tg_del_{canal}"),
                btn("❌ Cancelar", "menu_canais_tg")]), "HTML")
    elif data.startswith("tg_del_"):
        canal = data[7:]
        a = db.get_assinante(uid)
        if a:
            canais = [c for c in a.get("canais_tg",[]) if c != canal]
            ativos = [c for c in a.get("canais_tg_ativos",[]) if c != canal]
            db.set_canais_tg(uid, canais, ativos)
        await tela_canais_tg(update, ctx)
    
    # ── Grupos WhatsApp ───────────────────────────────────────
    elif data == "menu_grupos_wa":
        await tela_grupos_wa(update, ctx)
    elif data == "wa_connect":
        ctx.user_data["aguardando"] = "wa_telefone"
        await reply(update,
            "📱 <b>Conectar WhatsApp</b>\n\n"
            "Digite seu número com DDI+DDD (só números):\n"
            "Ex: <code>5511999998888</code>",
            kb([btn("❌ Cancelar","menu_grupos_wa")]), "HTML")
    elif data == "wa_confirm_logout":
        await reply(update,
            "⚠️ <b>Desconectar WhatsApp?</b>\n\nSeus grupos serão removidos.",
            kb([btn("✅ Sim","wa_logout"),btn("❌ Cancelar","menu_grupos_wa")]), "HTML")
    elif data == "wa_logout":
        await wa_logout(uid)
        db.set_grupos_wa(uid, [], [], {})
        await reply(update, "✅ <b>WhatsApp desconectado!</b>",
                    kb([btn("📲 WhatsApp","menu_grupos_wa")]), "HTML")
    elif data == "wa_listar":
        ctx.user_data.pop("wa_grupos_cache", None)  # força rebuscar
        await tela_listar_grupos_wa(update, ctx, 0)
    elif data.startswith("wa_lista_pag_"):
        pagina = int(data.split("_")[-1])
        await tela_listar_grupos_wa(update, ctx, pagina)
    elif data.startswith("wa_add_id_"):
        grupo  = data[10:]
        a      = db.get_assinante(uid)
        grupos = a.get("grupos_wa",[]) if a else []
        ativos = a.get("grupos_wa_ativos",[]) if a else []
        nomes  = db.get_nomes_grupos_wa(uid)
        limite_wa = db.get_limite_plano(uid, "grupos_wa")
        if grupo not in grupos and len(grupos) < limite_wa:
            grupos.append(grupo); ativos.append(grupo)
            db.set_grupos_wa(uid, grupos, ativos, nomes)
            await q.answer("✅ Adicionado!")
        elif len(grupos) >= limite_wa:
            await q.answer(f"❌ Limite de {limite_wa} grupos do plano")
        else:
            await q.answer("⚠️ Já na lista")
        await tela_listar_grupos_wa(update, ctx)
    elif data.startswith("wa_toggle_"):
        grupo = data[10:]
        novo  = db.toggle_grupo_wa(uid, grupo)
        await q.answer(f"{'🟢 Ativado' if novo else '🔴 Pausado'}")
        # Atualiza só o texto do botão sem recarregar tudo
        try:
            await tela_grupos_wa(update, ctx)
        except Exception:
            pass
    elif data.startswith("wa_confirm_del_"):
        grupo = data[15:]
        nomes = db.get_nomes_grupos_wa(uid)
        nome  = nomes.get(grupo) or _nome_curto_grupo(grupo)
        await reply(update,
            f"🗑️ <b>Remover grupo?</b>\n\n<b>{nome}</b>",
            kb([btn("✅ Sim, remover", f"wa_del_{grupo}"),
                btn("❌ Cancelar","menu_grupos_wa")]), "HTML")
    elif data.startswith("wa_del_"):
        grupo = data[7:]
        a = db.get_assinante(uid)
        if a:
            grupos = [g for g in a.get("grupos_wa",[]) if g != grupo]
            ativos = [g for g in a.get("grupos_wa_ativos",[]) if g != grupo]
            nomes  = db.get_nomes_grupos_wa(uid)
            db.set_grupos_wa(uid, grupos, ativos, nomes)
        await tela_grupos_wa(update, ctx)
    
    # ── Afiliados ─────────────────────────────────────────────
    elif data.startswith("aff_pular_"):
        # ex: aff_pular_link_shopee  ou  aff_pular_auto_shopee
        partes  = data.split("_")
        origem  = partes[2] if len(partes) > 2 else "link"
        ctx.user_data.pop("aguardando_aff", None)
        ctx.user_data.pop("aff_origem", None)
        ctx.user_data.pop("aff_plat_pendente", None)
        if origem == "link":
            url_pendente = ctx.user_data.pop("url_pendente_aff", "")
            if url_pendente:
                await q.answer("⚠️ Continuando sem comissão...")
                await processar_link(update, ctx, url_pendente)
            else:
                await cmd_start(update, ctx)
        else:  # auto
            a = db.get_assinante(uid)
            db.set_modo_auto(uid, True, a.get("min_desconto", 20) if a else 20)
            await q.answer("🤖 Auto-poster ativado (sem comissão)")
            await tela_auto(update, ctx)
    
    elif data == "menu_afiliados":
        await tela_afiliados(update, ctx)
    elif data.startswith("aff_edit_"):
        plataforma = data[9:]
        ctx.user_data["aguardando_aff"] = plataforma
        await reply(update,
            f"🔗 <b>Afiliado – {plataforma.capitalize()}</b>\n\n"
            "Digite seu código (só o código, sem URL):",
            kb([btn("❌ Cancelar","menu_afiliados")]), "HTML")
    
    # ── Modo Automático ───────────────────────────────────────
    elif data == "menu_auto":
        await tela_auto(update, ctx)
    elif data == "auto_on":
        # Verificar se tem afiliado Shopee cadastrado (principal plataforma do auto-poster)
        codigo_shopee = db.get_aff_code(uid, "shopee")
        if not codigo_shopee:
            await q.answer("⚠️ Configure seu ID de afiliado primeiro!")
            await tela_cadastro_afiliado(update, ctx, "shopee", origem="auto")
        else:
            a = db.get_assinante(uid)
            db.set_modo_auto(uid, True, a.get("min_desconto",20) if a else 20)
            await q.answer("🤖 Automático ATIVADO!")
            await tela_auto(update, ctx)
    elif data == "auto_off":
        db.set_modo_auto(uid, False)
        await q.answer("👤 Automático desativado.")
        await tela_auto(update, ctx)
    elif data.startswith("auto_min_"):
        min_desc = int(data[9:])
        a = db.get_assinante(uid)
        db.set_modo_auto(uid, bool(a.get("modo_auto",0)) if a else False, min_desc)
        await q.answer(f"✅ Mínimo: {min_desc}%")
        await tela_auto(update, ctx)
    elif data == "menu_cat_auto":
        await tela_categoria_auto(update, ctx)
    elif data.startswith("cat_auto_"):
        cat = data[9:]
        db.set_categoria_auto(uid, cat)
        await q.answer(f"✅ {CATEGORIAS_AUTO.get(cat,cat)}")
        await tela_auto(update, ctx)
    
    # ── Links ─────────────────────────────────────────────────
    elif data == "menu_links":
        await tela_links(update, ctx)
    elif data.startswith("lk_confirm_del_"):
        lid = int(data[15:])
        await reply(update, "🗑️ <b>Remover este link?</b>",
                    kb([btn("✅ Sim", f"lk_del_{lid}"),
                        btn("❌ Não","menu_links")]), "HTML")
    elif data.startswith("lk_del_"):
        db.remover_link(int(data[7:]), uid)
        await q.answer("🗑️ Removido!")
        await tela_links(update, ctx)
    elif data.startswith("lk_post_force_"):
        await postar_da_biblioteca(update, ctx, int(data[14:]), force=True)
    elif data.startswith("lk_post_"):
        await postar_da_biblioteca(update, ctx, int(data[8:]))
    elif data.startswith("lk_agendar_"):
        lid = int(data[11:])
        ctx.user_data["aguardando"]      = "horario_agenda"
        ctx.user_data["agendar_link_id"] = lid
        hors = "  ".join(f"<code>{h}</code>" for h in cfg.HORARIOS_POSTAGEM)
        await reply(update, f"⏰ Horários disponíveis:\n{hors}\n\nDigite <code>HH:MM</code>:",
                    kb([btn("❌ Cancelar","menu_agenda")]), "HTML")
    
    # ── Agenda ────────────────────────────────────────────────
    elif data == "menu_agenda":
        await tela_agenda(update, ctx)
    elif data.startswith("ag_confirm_del_"):
        aid = int(data[15:])
        await reply(update, "❌ <b>Cancelar este agendamento?</b>",
                    kb([btn("✅ Sim", f"ag_del_{aid}"),
                        btn("❌ Não","menu_agenda")]), "HTML")
    elif data.startswith("ag_del_"):
        db.cancelar_agendamento(int(data[7:]), uid)
        await q.answer("✅ Cancelado!")
        await tela_agenda(update, ctx)
    
    # ── Historico ─────────────────────────────────────────────
    elif data == "menu_historico":
        await tela_historico(update, ctx, 0)
    elif data.startswith("hist_pag_"):
        await tela_historico(update, ctx, int(data[9:]))
    elif data.startswith("repost_"):
        await repostar_historico(update, ctx, int(data[7:]))
    
    # ── Blacklist ─────────────────────────────────────────────
    elif data == "menu_blacklist":
        await tela_blacklist(update, ctx)
    elif data == "bl_add_loja":
        ctx.user_data["aguardando"] = "bl_loja"
        await reply(update, "🚫 Digite o nome da <b>loja</b> para bloquear:",
                    kb([btn("❌ Cancelar","menu_blacklist")]), "HTML")
    elif data == "bl_add_palavra":
        ctx.user_data["aguardando"] = "bl_palavra"
        await reply(update, "🚫 Digite a <b>palavra</b> para bloquear no título:",
                    kb([btn("❌ Cancelar","menu_blacklist")]), "HTML")
    elif data.startswith("bl_del_loja_"):
        idx = int(data[12:])
        a   = db.get_assinante(uid)
        bl  = a.get("blacklist_lojas",[]) if a else []
        if 0 <= idx < len(bl):
            nome = bl[idx]
            db.remove_blacklist_loja(uid, nome)
            await q.answer(f"✅ {nome} removida")
        await tela_blacklist(update, ctx)
    elif data.startswith("bl_del_palavra_"):
        idx = int(data[15:])
        a   = db.get_assinante(uid)
        bl  = a.get("blacklist_produtos",[]) if a else []
        if 0 <= idx < len(bl):
            bl.pop(idx)
            db._exec("UPDATE assinantes SET blacklist_produtos=%s WHERE id=%s",
                     (json.dumps(bl), uid))
            await q.answer("✅ Removida")
        await tela_blacklist(update, ctx)
    
    # ── Templates ─────────────────────────────────────────────
    elif data == "menu_templates":
        await tela_templates(update, ctx)
    elif data == "tmpl_novo":
        ctx.user_data["aguardando"] = "tmpl_nome"
        await reply(update,
            "📝 <b>Novo Template</b>\n\nDigite um <b>nome</b> para identificar o template:",
            kb([btn("❌ Cancelar","menu_templates")]), "HTML")
    elif data.startswith("tmpl_ver_"):
        idx       = int(data[9:])
        templates = db.get_templates_custom(uid)
        if 0 <= idx < len(templates):
            t = templates[idx]
            await reply(update,
                f"📝 <b>{t['nome']}</b>\n\n<code>{t['template'][:500]}</code>",
                kb([btn(f"🗑️ Excluir",f"tmpl_del_{idx}"),
                    btn("🔙 Voltar","menu_templates")]), "HTML")
    elif data.startswith("tmpl_del_"):
        idx = int(data[9:])
        db.remove_template_custom(uid, idx)
        await q.answer("🗑️ Removido!")
        await tela_templates(update, ctx)
    
    # ── Stats / Outros ────────────────────────────────────────
    elif data == "menu_stats":
        await tela_stats(update, ctx)
    elif data == "menu_estilo":
        await tela_estilo(update, ctx)
    elif data == "noop":
        pass  # botão separador
    elif data.startswith("usar_estilo_"):
        # usar_estilo_{url_hash}_{estilo}
        resto  = data[12:]  # remove "usar_estilo_"
        # estilo é a última parte, url_hash pode conter _
        # estilos conhecidos: padrao, urgencia, engracado, tecnico, sedutor, informativo
        estilo_key = None
        url_hash_k = resto
        for k in ESTILOS:
            if resto.endswith("_" + k):
                estilo_key = k
                url_hash_k = resto[:-(len(k)+1)]
                break
        if estilo_key:
            produto = ctx.user_data.get(f"prod_{url_hash_k}")
            if produto:
                db.limpar_copy_custom(uid, url_hash_k)
                ctx.user_data.pop(f"copy_custom_{url_hash_k}", None)
                copy = gerar_copy(produto, estilo_key)
                nome = ESTILOS[estilo_key]["nome"]
                await aplicar_copy_e_mostrar(update, ctx, url_hash_k, copy, nome)
            else:
                await reply(update, "⏳ Sessão expirada.", kb(VOLTAR_MAIN[0]))
        else:
            await q.answer("❌ Estilo não encontrado")
    elif data.startswith("usar_tmpl_"):
        # usar_tmpl_{url_hash}_{idx}
        resto = data[10:]
        # idx é o último segmento numérico
        partes    = resto.rsplit("_", 1)
        url_hash_k = partes[0]
        idx        = int(partes[1]) if len(partes) > 1 and partes[1].isdigit() else -1
        produto    = ctx.user_data.get(f"prod_{url_hash_k}")
        templates  = db.get_templates_custom(uid)
        if produto and 0 <= idx < len(templates):
            t    = templates[idx]
            copy = gerar_copy(produto, "padrao", t.get("template",""))
            # Ativa este template para o auto-poster também
            db.set_template_ativo(uid, idx)
            await aplicar_copy_e_mostrar(update, ctx, url_hash_k, copy, f"📝 {t['nome']}")
        else:
            await reply(update, "⏳ Sessão expirada.", kb(VOLTAR_MAIN[0]))
    elif data.startswith("estilo_tmpl_"):
        idx = int(data[12:])
        templates = db.get_templates_custom(uid)
        if 0 <= idx < len(templates):
            await tela_aplicar_copy_destinos(update, ctx, "tmpl", str(idx))
        else:
            await tela_estilo(update, ctx)
    elif data == "criar_copy_estilo":
        ctx.user_data["aguardando"] = "novo_estilo_nome"
        await reply(update,
            "📝 <b>Criar Copy Personalizada</b>\n\n"
            "Esta copy será usada no <b>auto-poster</b> e nas postagens manuais.\n\n"
            "1️⃣ Primeiro, digite um <b>nome</b> para identificar:\n"
            "Ex: <code>Moda Feminina</code> ou <code>Eletrônicos</code>",
            kb([btn("❌ Cancelar","menu_estilo")]), "HTML")
    # ── Aplicar copy em destino específico ───────────────────
    elif data.startswith("copy_apply_all_"):
        # copy_apply_all_{tipo}_{ref}
        resto     = data[15:]
        tipo_copy = "tmpl" if resto.startswith("tmpl_") else "estilo"
        copy_ref  = resto[5:] if tipo_copy == "tmpl" else resto[7:]
        a         = db.get_assinante(uid)
        canais    = a.get("canais_tg", []) if a else []
        grupos    = a.get("grupos_wa", []) if a else []
        templates = db.get_templates_custom(uid)
        if tipo_copy == "estilo":
            db.set_estilo(uid, copy_ref)
            db.set_template_ativo(uid, -1)
            for c in canais: db.set_template_canal_tg(uid, c, -1)
            for g in grupos: db.set_template_grupo_wa(uid, g, -1)
            nome = ESTILOS.get(copy_ref,{}).get("nome", copy_ref)
        else:
            idx = int(copy_ref)
            db.set_template_ativo(uid, idx)
            for c in canais: db.set_template_canal_tg(uid, c, idx)
            for g in grupos: db.set_template_grupo_wa(uid, g, idx)
            nome = templates[idx]["nome"] if 0 <= idx < len(templates) else "?"
        await q.answer(f"✅ {nome} aplicado em todos!")
        await tela_estilo(update, ctx)
    
    elif data.startswith("copy_apply_tg_"):
        # copy_apply_tg_{canal}_{tipo}_{ref}
        # tipo é "tmpl" ou "estilo", ref é o valor
        resto = data[14:]
        tipo_copy = "tmpl" if "_tmpl_" in resto else "estilo"
        sep   = "_tmpl_" if tipo_copy == "tmpl" else "_estilo_"
        partes = resto.split(sep, 1)
        canal    = partes[0]
        copy_ref = partes[1] if len(partes) > 1 else ""
        templates = db.get_templates_custom(uid)
        if tipo_copy == "tmpl":
            idx = int(copy_ref)
            # Toggle: se já usa, remove; senão aplica
            atual = db.get_templates_tg(uid).get(canal, -1)
            if atual == idx:
                db.set_template_canal_tg(uid, canal, -1)
                await q.answer("❌ Removido deste canal")
            else:
                db.set_template_canal_tg(uid, canal, idx)
                nome = templates[idx]["nome"] if 0 <= idx < len(templates) else "?"
                await q.answer(f"✅ {nome}")
        else:
            db.set_estilo(uid, copy_ref)
            db.set_template_canal_tg(uid, canal, -1)
            await q.answer(f"✅ {ESTILOS.get(copy_ref,{}).get('nome',copy_ref)}")
        await tela_aplicar_copy_destinos(update, ctx,
            ctx.user_data.get("copy_aplicar_tipo", tipo_copy),
            ctx.user_data.get("copy_aplicar_ref", copy_ref))
    
    elif data.startswith("copy_apply_wa_"):
        # copy_apply_wa_{grupo}_{tipo}_{ref}
        resto = data[14:]
        tipo_copy = "tmpl" if "_tmpl_" in resto else "estilo"
        sep   = "_tmpl_" if tipo_copy == "tmpl" else "_estilo_"
        partes = resto.split(sep, 1)
        grupo    = partes[0]
        copy_ref = partes[1] if len(partes) > 1 else ""
        templates = db.get_templates_custom(uid)
        if tipo_copy == "tmpl":
            idx = int(copy_ref)
            atual = db.get_templates_wa(uid).get(grupo, -1)
            if atual == idx:
                db.set_template_grupo_wa(uid, grupo, -1)
                await q.answer("❌ Removido deste grupo")
            else:
                db.set_template_grupo_wa(uid, grupo, idx)
                nome = templates[idx]["nome"] if 0 <= idx < len(templates) else "?"
                await q.answer(f"✅ {nome}")
        else:
            db.set_estilo(uid, copy_ref)
            db.set_template_grupo_wa(uid, grupo, -1)
            await q.answer(f"✅ {ESTILOS.get(copy_ref,{}).get('nome',copy_ref)}")
        await tela_aplicar_copy_destinos(update, ctx,
            ctx.user_data.get("copy_aplicar_tipo", tipo_copy),
            ctx.user_data.get("copy_aplicar_ref", copy_ref))
    
    elif data == "menu_nichos":
        await tela_nichos(update, ctx)
    elif data.startswith("nicho_sel_"):
        nicho = data[10:]
        await tela_nicho_destinos(update, ctx, nicho)
    elif data.startswith("nicho_toggle_tg_"):
        # nicho_toggle_tg_{canal}_{nicho} — toggle (add/remove) nicho da lista
        resto = data[16:]
        nicho = resto.rsplit("_", 1)[1]
        canal = resto.rsplit("_", 1)[0]
        lista_atual = db.get_nichos_tg(uid).get(canal, [])
        db.set_nicho_tg(uid, canal, nicho)  # faz toggle
        nova_lista = db.get_nichos_tg(uid).get(canal, [])
        if nicho in nova_lista:
            await q.answer(f"✅ {CATEGORIAS_AUTO.get(nicho, nicho)} adicionado!")
        else:
            await q.answer(f"⬜ {CATEGORIAS_AUTO.get(nicho, nicho)} removido")
        await tela_nicho_destinos(update, ctx, nicho)
    elif data.startswith("nicho_toggle_wa_"):
        # nicho_toggle_wa_{grupo}_{nicho} — toggle (add/remove) nicho da lista
        resto = data[16:]
        nicho = resto.rsplit("_", 1)[1]
        grupo = resto.rsplit("_", 1)[0]
        db.set_nicho_wa(uid, grupo, nicho)  # faz toggle
        nova_lista = db.get_nichos_wa(uid).get(grupo, [])
        if nicho in nova_lista:
            await q.answer(f"✅ {CATEGORIAS_AUTO.get(nicho, nicho)} adicionado!")
        else:
            await q.answer(f"⬜ {CATEGORIAS_AUTO.get(nicho, nicho)} removido")
        await tela_nicho_destinos(update, ctx, nicho)
    elif data == "menu_nichos":
        await tela_nichos(update, ctx)
    elif data.startswith("nicho_ver_"):
        await tela_nicho_destinos(update, ctx, data[10:])
    elif data.startswith("nicho_pag_"):
        # nicho_pag_{nicho}_{pagina}
        resto  = data[10:]
        partes = resto.rsplit("_", 1)
        nicho_pag = partes[0]
        pag       = int(partes[1]) if len(partes) > 1 and partes[1].isdigit() else 0
        await tela_nicho_destinos(update, ctx, nicho_pag, pag)
    elif data.startswith("nicho_apply_all_"):
        nicho = data[16:]
        a = db.get_assinante(uid)
        for c in (a.get("canais_tg", []) if a else []):
            if nicho not in db.get_nichos_tg(uid).get(c, []):
                db.set_nicho_tg(uid, c, nicho)
        for g in (a.get("grupos_wa", []) if a else []):
            if nicho not in db.get_nichos_wa(uid).get(g, []):
                db.set_nicho_wa(uid, g, nicho)
        nome = CATEGORIAS_AUTO.get(nicho, nicho)
        await q.answer(f"✅ {nome} adicionado em todos!")
        await tela_nicho_destinos(update, ctx, nicho)
    elif data.startswith("nicho_remove_all_"):
        nicho = data[17:]
        a = db.get_assinante(uid)
        for c in (a.get("canais_tg", []) if a else []):
            if nicho in db.get_nichos_tg(uid).get(c, []):
                db.set_nicho_tg(uid, c, nicho)  # toggle remove
        for g in (a.get("grupos_wa", []) if a else []):
            if nicho in db.get_nichos_wa(uid).get(g, []):
                db.set_nicho_wa(uid, g, nicho)  # toggle remove
        nome = CATEGORIAS_AUTO.get(nicho, nicho)
        await q.answer(f"🗑️ {nome} removido de todos!")
        await tela_nicho_destinos(update, ctx, nicho)
    elif data == "menu_referral":
        await tela_referral(update, ctx)
    elif data == "menu_config":
        await tela_config(update, ctx)
    elif data == "menu_ajuda":
        await tela_ajuda(update, ctx)
    elif data == "config_email":
        ctx.user_data["aguardando"] = "email"
        await reply(update, "📧 Digite seu email:",
                    kb([btn("❌ Cancelar","menu_config")]), "HTML")
    elif data.startswith("estilo_") and not data.startswith("estilo_tmpl_"):
        estilo = data[7:]
        if estilo in ESTILOS:
            await tela_aplicar_copy_destinos(update, ctx, "estilo", estilo)
        else:
            await tela_estilo(update, ctx)
    
    # ── Postagem ──────────────────────────────────────────────
    elif data.startswith("dest_sel_"):
        await tela_escolher_destino(update, ctx, data[9:])

    elif data.startswith("dest_tg_"):
        # dest_tg_{url_hash}_{canal_id}  — toggle canal TG na seleção
        resto    = data[8:]
        url_hash = resto[:14]          # hash sempre tem 14 chars
        canal    = resto[15:]          # canal vem após o hash + "_"
        key_sel  = f"destinos_sel_{url_hash}"
        sel      = ctx.user_data.setdefault(key_sel, {"tg": [], "wa": []})
        if canal in sel["tg"]:
            sel["tg"].remove(canal)
            await q.answer("⬜ Removido")
        else:
            sel["tg"].append(canal)
            await q.answer("✅ Adicionado")
        await tela_escolher_destino(update, ctx, url_hash)

    elif data.startswith("dest_wa_"):
        # dest_wa_{url_hash}_{grupo_id}
        resto    = data[8:]
        url_hash = resto[:14]
        grupo    = resto[15:]
        key_sel  = f"destinos_sel_{url_hash}"
        sel      = ctx.user_data.setdefault(key_sel, {"tg": [], "wa": []})
        if grupo in sel["wa"]:
            sel["wa"].remove(grupo)
            await q.answer("⬜ Removido")
        else:
            sel["wa"].append(grupo)
            await q.answer("✅ Adicionado")
        await tela_escolher_destino(update, ctx, url_hash)

    elif data.startswith("dest_all_tg_"):
        url_hash = data[12:]
        a        = db.get_assinante(uid)
        canais   = a.get("canais_tg", []) if a else []
        key_sel  = f"destinos_sel_{url_hash}"
        sel      = ctx.user_data.setdefault(key_sel, {"tg": [], "wa": []})
        sel["tg"] = list(canais)
        await q.answer("✅ Todos TG selecionados")
        await tela_escolher_destino(update, ctx, url_hash)

    elif data.startswith("dest_none_tg_"):
        url_hash = data[13:]
        key_sel  = f"destinos_sel_{url_hash}"
        sel      = ctx.user_data.setdefault(key_sel, {"tg": [], "wa": []})
        sel["tg"] = []
        await q.answer("⬜ TG desmarcados")
        await tela_escolher_destino(update, ctx, url_hash)

    elif data.startswith("dest_all_wa_"):
        url_hash = data[12:]
        a        = db.get_assinante(uid)
        grupos   = a.get("grupos_wa", []) if a else []
        key_sel  = f"destinos_sel_{url_hash}"
        sel      = ctx.user_data.setdefault(key_sel, {"tg": [], "wa": []})
        sel["wa"] = list(grupos)
        await q.answer("✅ Todos WA selecionados")
        await tela_escolher_destino(update, ctx, url_hash)

    elif data.startswith("dest_none_wa_"):
        url_hash = data[13:]
        key_sel  = f"destinos_sel_{url_hash}"
        sel      = ctx.user_data.setdefault(key_sel, {"tg": [], "wa": []})
        sel["wa"] = []
        await q.answer("⬜ WA desmarcados")
        await tela_escolher_destino(update, ctx, url_hash)

    elif data.startswith("dest_confirm_"):
        await executar_postagem_destinos_selecionados(update, ctx, data[13:])

    elif data.startswith("post_tg_force_"):
        await executar_postagem_force(update, ctx, data[14:], "tg")
    elif data.startswith("post_wa_force_"):
        await executar_postagem_force(update, ctx, data[14:], "wa")
    elif data.startswith("post_all_force_"):
        await executar_postagem_force(update, ctx, data[15:], "ambos")
    elif data.startswith("post_tg_"):
        await executar_postagem_telegram(update, ctx, data[8:])
    elif data.startswith("post_wa_"):
        await executar_postagem_whatsapp(update, ctx, data[8:])
    elif data.startswith("post_all_"):
        await executar_postagem_ambos(update, ctx, data[9:])
    elif data.startswith("salvar_"):
        await salvar_produto(update, ctx, data[7:])
    elif data.startswith("edit_copy_"):
        await tela_editar_copy(update, ctx, data[10:])
    elif data.startswith("recopy_"):
        # Limpa copy customizada ao gerar nova automática
        ctx.user_data.pop(f"copy_custom_{data[7:]}", None)
        db.limpar_copy_custom(uid, data[7:])
        await regerar_copy(update, ctx, data[7:])
    elif data.startswith("agendar_prod_"):
        url_hash = data[13:]
        ctx.user_data["aguardando"]   = "horario_prod"
        ctx.user_data["hash_produto"] = url_hash
        hors = "  ".join(f"<code>{h}</code>" for h in cfg.HORARIOS_POSTAGEM)
        await reply(update,
            f"⏰ Horários:\n{hors}\n\nDigite <code>HH:MM</code>:",
            kb([btn("❌ Cancelar","main_menu")]), "HTML")
    elif data == "menu_postar":
        await reply(update,
            "📤 <b>Postar Produto</b>\n\n"
            "Envie o link do produto aqui no chat!\n\n"
            "<i>Shopee • Amazon • Magalu • AliExpress • ML • Hotmart • Monetizze</i>",
            kb(VOLTAR_MAIN[0]), "HTML")
    
    
    # ══════════════════════════════════════════════════════════════
    #  PAINEL ADMIN
    # ══════════════════════════════════════════════════════════════
async def cmd_assinantes(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Comando admin: ver todos os assinantes (consulta Neon)"""
    if not is_admin(update.effective_user.id):
        return
    
    try:
        async with aiohttp.ClientSession() as session:
            # Busca usuários ativos no Neon
            async with session.get('https://botautomacao.vercel.app/assinantes/ativos', timeout=10) as resp:
                if resp.status == 200:
                    usuarios_ativos = await resp.json()
                else:
                    usuarios_ativos = []
            
            # Busca todos os usuários do Neon (para contar testes)
            async with session.get('https://botautomacao.vercel.app/assinantes/todos', timeout=10) as resp:
                if resp.status == 200:
                    todos_usuarios = await resp.json()
                else:
                    todos_usuarios = []
    
    except Exception as e:
        await update.message.reply_text(f"❌ Erro ao consultar Neon: {e}")
        return
    
    # Normaliza datas vindas do Neon (podem vir em vários formatos)
    hoje = datetime.now().date()

    ativos = []
    for u in usuarios_ativos:
        venc_str = Database._parse_date(u.get('vencimento', ''))
        if venc_str:
            try:
                if datetime.strptime(venc_str, '%Y-%m-%d').date() >= hoje:
                    u['vencimento'] = venc_str
                    ativos.append(u)
            except Exception:
                pass

    vencidos = []
    for u in todos_usuarios:
        venc_str = Database._parse_date(u.get('vencimento', ''))
        if venc_str:
            try:
                if datetime.strptime(venc_str, '%Y-%m-%d').date() < hoje:
                    u['vencimento'] = venc_str
                    vencidos.append(u)
            except Exception:
                pass

    em_teste = [u for u in todos_usuarios if u.get('plano') == 'teste']

    texto = (
        f"👥 <b>Assinantes do Bot</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"✅ Ativos:    <b>{len(ativos)}</b>\n"
        f"❌ Vencidos:  <b>{len(vencidos)}</b>\n"
        f"🧪 Em teste:  <b>{len(em_teste)}</b>\n"
        f"💰 Receita:   <b>R$ {db.admin_overview()['receita']:.2f}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
    )

    if ativos:
        texto += f"<b>Ativos ({len(ativos)}):</b>\n"
        for a in ativos[:20]:
            uid  = a['id']
            nome = a.get('nome') or str(uid)
            venc = Database._parse_date(a.get('vencimento', ''))
            try:
                dias = (datetime.strptime(venc, '%Y-%m-%d') - datetime.now()).days
                texto += f"  🤖 @{nome} (<code>{uid}</code>)\n"
                texto += f"      📅 {venc} ({dias}d)\n"
            except Exception:
                texto += f"  🤖 @{nome} (<code>{uid}</code>)\n"
        if len(ativos) > 20:
            texto += f"  <i>... +{len(ativos)-20} mais</i>\n"

    await update.message.reply_text(texto, parse_mode="HTML")

def calcular_receita() -> float:
    """Receita real: soma de pagamentos paid no NeonDB."""
    try:
        row = db._exec(
            "SELECT COALESCE(SUM(valor),0.0) as total FROM pagamentos WHERE status='paid'",
            fetch="one")
        return float(row["total"]) if row else 0.0
    except Exception:
        return 0.0

async def cmd_admin(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    ov = db.admin_overview()
    texto = (
        f"🛡️ <b>Painel Admin</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"👥 Assinantes: <b>{ov['assinantes']}</b>\n"
        f"🧪 Em teste:   <b>{ov['testes']}</b>\n"
        f"💰 Vendas:     <b>{ov['vendas']}</b>  R${ov['receita']:.2f}\n"
        f"📧 Emails:     <b>{ov['emails']}</b>\n\n"
        f"📬 Posts TG:   <b>{ov['postagens']}</b>\n"
        f"📲 Posts WA:   <b>{ov['wa_posts']}</b>\n"
        f"🤖 AutoPosts:  <b>{ov['auto_posts']}</b>\n"
        f"📅 Hoje:       <b>{ov['hoje']}</b>\n"
        f"🔗 Links:      <b>{ov['links']}</b>\n"
        f"🤝 Referrals:  <b>{ov['referrals']}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<i>Comandos admin abaixo 👇</i>"
    )
    rows = [
        [btn("👥 Listar Assinantes",   "adm_listar")],
        [btn("➕ Ativar Usuário",       "adm_ativar")],
        [btn("❌ Desativar",            "adm_desativar")],
        [btn("📢 Broadcast",           "adm_broadcast")],
        [btn("🔔 Notificações",        "adm_notifs")],
        [btn("📊 Vencendo em 3 dias",  "adm_vencendo")],
    ]
    await reply(update, texto, InlineKeyboardMarkup(rows), "HTML")
    
    
async def callback_admin(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q    = update.callback_query
    uid  = q.from_user.id
    if not is_admin(uid): return
    data = q.data
    await q.answer()
    
    if data == "adm_listar":
        assinantes = db.listar_assinantes()
        if not assinantes:
            await reply(update, "Nenhum assinante.", kb([btn("🔙 Voltar","cmd_admin")])); return
        linhas = [f"👥 <b>Assinantes ({len(assinantes)})</b>\n"]
        for a in assinantes[:20]:
            plano   = PLANOS.get(a.get("plano","mensal"),{}).get("nome","?")
            canais  = len(a.get("canais_tg",[]))
            nome    = a.get("username") or a.get("nome") or str(a["id"])
            linhas.append(f"  • <code>{a['id']}</code> @{nome} [{plano}] "
                          f"Vence: {a['vencimento']} 📢{canais}")
        await reply(update, "\n".join(linhas), kb([btn("🔙 Painel","/admin")]), "HTML")
    
    elif data == "adm_vencendo":
        vencendo = db.assinantes_vencendo(dias=3)
        if not vencendo:
            await reply(update, "✅ Nenhum vencendo em 3 dias.", kb([btn("🔙","/admin")]))
            return
        linhas = [f"⚠️ <b>Vencendo ({len(vencendo)})</b>\n"]
        for a in vencendo:
            linhas.append(f"  • <code>{a['id']}</code> → {a['vencimento']}")
        await reply(update, "\n".join(linhas), kb([btn("🔙","/admin")]), "HTML")
    
    elif data == "adm_notifs":
        notifs = db.notifs_pendentes()
        if not notifs:
            await reply(update, "✅ Sem notificações.", kb([btn("🔙","/admin")])); return
        linhas = [f"🔔 <b>Notificações ({len(notifs)})</b>\n"]
        for n in notifs[:10]:
            linhas.append(f"  [{n['tipo']}] uid={n['user_id']} – {n['mensagem'][:60]}")
        await reply(update, "\n".join(linhas), kb([btn("🔙","/admin")]), "HTML")
    
    
async def cmd_corrigir_ativos(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    Admin: reativa todos os usuários com vencimento futuro mas ativo=FALSE.
    Corrige o bug do VerificadorAssinaturas que desativava datas no formato YYYY-MM.
    """
    if not is_admin(update.effective_user.id): return
    msg = await update.message.reply_text("🔍 Verificando assinantes incorretamente desativados...")

    hoje = datetime.now().strftime("%Y-%m-%d")
    # Busca INATIVOS com vencimento que ainda não expirou
    rows = db._exec(
        "SELECT id, nome, vencimento FROM assinantes WHERE ativo=0",
        fetch="all") or []

    reativados = []
    for row in rows:
        uid      = row["id"]
        venc_raw = row.get("vencimento") or ""
        venc_str = Database._parse_date(venc_raw)
        if venc_str and venc_str >= hoje:
            # Vencimento futuro — foi desativado por engano
            db._exec("UPDATE assinantes SET ativo=1, modo_auto=0 WHERE id=%s", (uid,))
            nome = row.get("nome") or str(uid)
            reativados.append((uid, nome, venc_str))
            logger.info(f"[Correção] uid={uid} ({nome}) reativado — vence {venc_str}")
            # Notifica o usuário
            try:
                await ctx.bot.send_message(
                    uid,
                    f"✅ <b>Sua assinatura foi restaurada!</b>\n\n"
                    f"Identificamos um erro técnico que desativou sua conta incorretamente.\n\n"
                    f"📅 Válida até: <b>{venc_str}</b>\n\n"
                    f"Use /start para continuar! 🚀",
                    parse_mode="HTML")
            except Exception:
                pass

    if reativados:
        texto = f"✅ <b>{len(reativados)} usuário(s) reativado(s):</b>\n\n"
        for uid, nome, venc in reativados[:20]:
            texto += f"  • <code>{uid}</code> @{nome} → {venc}\n"
    else:
        texto = "✅ Nenhum usuário incorretamente desativado encontrado."

    await msg.edit_text(texto, parse_mode="HTML")


async def cmd_admin_ativar(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): 
        return
    
    args = (ctx.args or [])
    if len(args) < 2:
        await update.message.reply_text(
            "Uso: <code>/ativar &lt;uid&gt; &lt;dias&gt;</code>\n"
            "Ex: <code>/ativar 123456789 30</code>",
            parse_mode="HTML")
        return
    
    try:
        uid_alvo = int(args[0])
        dias     = int(args[1])
        plano    = "mensal"
    except (ValueError, IndexError):
        await update.message.reply_text("❌ Parâmetros inválidos.")
        return
    
    venc = db.ativar(uid_alvo, dias, plano)
    plano_nm = PLANOS.get(plano, {}).get("nome", plano.upper())
    
    # 🔥 NOTIFICA O USUÁRIO QUE FOI ATIVADO
    try:
        await ctx.bot.send_message(
            uid_alvo,
            f"✅ <b>Sua assinatura foi ativada!</b>\n\n"
            f"📋 Plano: <b>{plano_nm}</b>\n"
            f"📅 Válida até: <b>{venc}</b>\n\n"
            f"🤖 Use /start para acessar o bot! 🚀",
            parse_mode="HTML"
        )
        notificado = "✅ Usuário notificado!"
    except Exception as e:
        notificado = f"⚠️ Não foi possível notificar o usuário: {e}"
    
    # Notifica o admin
    await update.message.reply_text(
        f"✅ <b>Ativado!</b>\n"
        f"👤 UID: <code>{uid_alvo}</code>\n"
        f"📋 Plano: <b>{plano_nm}</b>\n"
        f"📅 Vencimento: <b>{venc}</b>\n\n"
        f"{notificado}",
        parse_mode="HTML")
    
    # Notifica o admin sobre a nova assinatura
    await Notif.nova_assinatura(uid_alvo, plano, venc, metodo="admin_manual")
    
    # Processa referral (indicação)
    ref_id = db.processar_referral(uid_alvo)
    if ref_id:
        try:
            await ctx.bot.send_message(
                ref_id,
                f"🎁 <b>Bônus de indicação!</b>\n"
                f"+{cfg.BONUS_CONVIDANTE} dias adicionados à sua assinatura! 🎉",
                parse_mode="HTML")
        except Exception:
            pass
    
async def cmd_admin_desativar(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    if not ctx.args:
        await update.message.reply_text("Uso: /desativar <uid>"); return
    try: uid_alvo = int(ctx.args[0])
    except ValueError:
        await update.message.reply_text("❌ UID inválido."); return
    db.desativar(uid_alvo)
    await update.message.reply_text(f"✅ <code>{uid_alvo}</code> desativado.", parse_mode="HTML")
    
    
async def _enviar_broadcast(bot, uid: int, texto: str) -> Tuple[bool, str]:
    """Tenta enviar mensagem para um usuário. Retorna (ok, motivo_falha)."""
    try:
        await bot.send_message(uid, f"📢 <b>Comunicado</b>\n\n{texto}", parse_mode="HTML")
        return True, ""
    except Exception as e:
        err = str(e).lower()
        if "blocked" in err or "bot was blocked" in err:
            return False, "bloqueou o bot"
        elif "chat not found" in err or "user not found" in err:
            return False, "chat não encontrado"
        elif "deactivated" in err:
            return False, "conta desativada"
        elif "forbidden" in err:
            return False, "acesso negado"
        else:
            return False, str(e)[:50]
    
    
async def cmd_broadcast(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    texto = " ".join(ctx.args or []).strip()
    if not texto:
        await update.message.reply_text(
            "Uso: <code>/broadcast mensagem aqui</code>\n"
            "Teste: <code>/broadcast_teste mensagem</code> — só quem está no teste",
            parse_mode="HTML"); return
    assinantes = db.listar_assinantes()
    msg = await update.message.reply_text(f"📢 Enviando para {len(assinantes)} usuários...")
    ok_ct = fail_ct = 0
    motivos: Dict[str, int] = {}
    for a in assinantes:
        ok, motivo = await _enviar_broadcast(ctx.bot, a["id"], texto)
        if ok:
            ok_ct += 1
        else:
            fail_ct += 1
            motivos[motivo] = motivos.get(motivo, 0) + 1
        await asyncio.sleep(0.08)  # respeita rate limit Telegram
    
    resumo = f"📊 <b>Broadcast concluído</b>\n\n✅ Enviados: <b>{ok_ct}</b>\n❌ Falhas: <b>{fail_ct}</b>"
    if motivos:
        resumo += "\n\n<b>Motivos das falhas:</b>"
        for motivo, qtd in sorted(motivos.items(), key=lambda x: -x[1]):
            resumo += f"\n  • {motivo}: {qtd}"
    await msg.edit_text(resumo, parse_mode="HTML")
    
    
async def cmd_broadcast_teste(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Broadcast apenas para usuários em período de teste."""
    if not is_admin(update.effective_user.id): return
    texto = " ".join(ctx.args or []).strip()
    if not texto:
        await update.message.reply_text("Uso: /broadcast_teste <mensagem>"); return
    
    # Busca usuários no teste ativo
    rows = db._exec("""
        SELECT t.user_id, a.nome, a.username FROM testes t
        LEFT JOIN assinantes a ON a.id = t.user_id
        WHERE t.fim >= CURRENT_DATE
    """, fetch="all") or []
    
    if not rows:
        await update.message.reply_text("Nenhum usuário em teste ativo."); return
    
    msg = await update.message.reply_text(f"🧪 Enviando para {len(rows)} usuários em teste...")
    ok_ct = fail_ct = 0
    motivos: Dict[str, int] = {}
    for row in rows:
        uid = row["user_id"] if isinstance(row, dict) else row[0]
        ok, motivo = await _enviar_broadcast(ctx.bot, uid, texto)
        if ok: ok_ct += 1
        else:
            fail_ct += 1
            motivos[motivo] = motivos.get(motivo, 0) + 1
        await asyncio.sleep(0.08)
    
    resumo = (f"📊 <b>Broadcast Teste concluído</b>\n\n"
              f"🧪 Total em teste: <b>{len(rows)}</b>\n"
              f"✅ Enviados: <b>{ok_ct}</b>\n"
              f"❌ Falhas: <b>{fail_ct}</b>")
    if motivos:
        resumo += "\n\n<b>Motivos:</b>"
        for motivo, qtd in sorted(motivos.items(), key=lambda x: -x[1]):
            resumo += f"\n  • {motivo}: {qtd}"
    await msg.edit_text(resumo, parse_mode="HTML")
    
    
async def cmd_pagou(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    Usuário usa /pagou após pagar — o bot consulta o Neon/MP e ativa se confirmado.
    Útil quando o webhook demora ou falha.
    """
    uid  = update.effective_user.id
    msg  = await update.message.reply_text("🔍 Verificando seu pagamento...")

    # 1) Já está ativo?
    if db.assinatura_ativa(uid):
        a    = db.get_assinante(uid)
        venc = Database._parse_date(a["vencimento"])
        await msg.edit_text(
            f"✅ <b>Assinatura já está ativa!</b>\n\n"
            f"📅 Válida até: <b>{venc}</b>\n\n"
            f"Use /start para acessar o bot.",
            parse_mode="HTML")
        return

    # 2) Consulta o Neon (API Vercel) como fallback
    ativado = False
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"https://botautomacao.vercel.app/verificar/{uid}",
                timeout=aiohttp.ClientTimeout(total=8)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("ativo"):
                        venc_raw  = data.get("vencimento","")
                        venc_str  = venc_raw.split("T")[0] if "T" in venc_raw else venc_raw
                        plano_str = data.get("plano","mensal")
                        db.ativar(uid, 30, plano_str)
                        ativado = True
                        await msg.edit_text(
                            f"🎉 <b>Assinatura ativada!</b>\n\n"
                            f"📋 Plano: <b>{PLANOS.get(plano_str,{}).get('nome','Mensal')}</b>\n"
                            f"📅 Válida até: <b>{venc_str or 'verificar /start'}</b>\n\n"
                            f"Use /start para acessar o bot! 🚀",
                            parse_mode="HTML")
    except Exception as e:
        logger.warning(f"[cmd_pagou] Neon check uid={uid}: {e}")

    if not ativado:
        await msg.edit_text(
            "⏳ <b>Pagamento ainda não confirmado.</b>\n\n"
            "O MercadoPago pode levar alguns minutos para processar.\n\n"
            "Se já pagou há mais de 10 minutos, entre em contato com o suporte:",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🆘 Suporte", url=cfg.SUPORTE_LINK)
            ]])
        )


async def cmd_meuid(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    await update.message.reply_text(f"🆔 Seu ID: <code>{uid}</code>", parse_mode="HTML")
    
    
async def cmd_email(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not db.assinatura_ativa(uid):
        await update.message.reply_text("❌ Sem assinatura ativa."); return
    if not ctx.args:
        await update.message.reply_text("Uso: /email <seu@email.com>"); return
    email = ctx.args[0].strip()
    if not re.match(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$", email):
        await update.message.reply_text("❌ Email inválido."); return
    db.salvar_email(email, uid)
    await Notif.novo_email(uid, email, update.effective_user.username)
    await update.message.reply_text(f"✅ Email: <code>{email}</code>", parse_mode="HTML")
    
async def cmd_produto(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Atalho: /produto <link> — processa diretamente sem navegar pelo menu."""
    uid = update.effective_user.id
    if not db.assinatura_ativa(uid):
        await update.message.reply_text("❌ Sem assinatura ativa. Use /start.")
        return
    if not ctx.args:
        await update.message.reply_text(
            "Uso: <code>/produto https://shopee.com.br/...</code>",
            parse_mode="HTML")
        return
    url = ctx.args[0]
    await processar_link(update, ctx, url)
    
async def cmd_list(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Alias para ver a biblioteca de links."""
    uid = update.effective_user.id
    if not db.assinatura_ativa(uid):
        await update.message.reply_text("❌ Sem assinatura ativa. Use /start.")
        return
    await tela_links(update, ctx)
    
async def cmd_ajuda(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await tela_ajuda(update, ctx)
    
    
    # ══════════════════════════════════════════════════════════════
    #  WEBHOOKS PAGAMENTO
    # ══════════════════════════════════════════════════════════════
flask_app = Flask(__name__)
    
    
@flask_app.route("/webhook/mercadopago", methods=["POST"])
def webhook_mp():
    """
    Webhook MercadoPago — versão robusta.
    ✅ Ativa assinatura automaticamente ao receber approved/authorized
    ✅ Idempotente (não processa o mesmo order_id duas vezes)
    ✅ Desativa ao receber cancelled/refunded/charged_back
    ✅ Notifica o usuário no Telegram em ambos os casos
    ✅ Tenta resolver UID por external_reference, email e metadata
    """
    try:
        dados   = flask_request.get_json(force=True, silent=True) or {}
        tipo    = dados.get("type", "")
        data_id = str(dados.get("data", {}).get("id", ""))

        logger.info(f"[MP Webhook] tipo={tipo} id={data_id}")
        Notif.webhook_sync(data_id, 0, tipo, "mercadopago")

        if tipo not in ("payment", "subscription_preapproval", "preapproval"):
            return jsonify({"status": "ignored"}), 200
        if not cfg.MP_ACCESS_TOKEN or not data_id:
            return jsonify({"status": "no_token_or_id"}), 200

        # ── Busca detalhes na API do MP ───────────────────────────
        if tipo == "payment":
            mp_url = f"https://api.mercadopago.com/v1/payments/{data_id}"
        else:
            mp_url = f"https://api.mercadopago.com/preapproval/{data_id}"

        headers  = {"Authorization": f"Bearer {cfg.MP_ACCESS_TOKEN}"}
        r        = requests.get(mp_url, headers=headers, timeout=15)
        if r.status_code != 200:
            logger.error(f"[MP Webhook] API retornou {r.status_code}: {r.text[:200]}")
            return jsonify({"status": "mp_error"}), 200

        pagamento = r.json()
        status    = pagamento.get("status", "").lower()
        ext_ref   = str(pagamento.get("external_reference", "") or "").strip()
        email     = str(pagamento.get("payer", {}).get("email", "") or "").lower().strip()
        valor     = float(
            pagamento.get("transaction_amount") or
            pagamento.get("auto_recurring", {}).get("transaction_amount") or 0
        )
        order_id  = f"mp_{data_id}"
        plano     = "mensal"

        logger.info(f"[MP Webhook] status={status} ext_ref='{ext_ref}' email='{email}' valor={valor}")

        # ── Resolve UID ───────────────────────────────────────────
        uid: Optional[int] = None

        # 1) external_reference = "uid" ou "uid_sufixo"
        if ext_ref:
            try:
                uid = int(ext_ref.split("_")[0])
                logger.info(f"[MP Webhook] UID via ext_ref: {uid}")
            except (ValueError, IndexError):
                logger.warning(f"[MP Webhook] ext_ref inválido: '{ext_ref}'")

        # 2) Email cadastrado no bot
        if not uid and email:
            uid = db.buscar_uid_por_email(email)
            if uid:
                logger.info(f"[MP Webhook] UID via email (emails table): {uid}")

        # 3) metadata.uid / telegram_id / user_id
        if not uid:
            meta     = pagamento.get("metadata", {}) or {}
            uid_meta = meta.get("uid") or meta.get("telegram_id") or meta.get("user_id")
            if uid_meta:
                try:
                    uid = int(uid_meta)
                    logger.info(f"[MP Webhook] UID via metadata: {uid}")
                except (ValueError, TypeError):
                    pass

        # 4) Busca por email na tabela de pagamentos anteriores
        if not uid and email:
            try:
                row = db._exec(
                    "SELECT user_id FROM pagamentos WHERE email=%s AND user_id IS NOT NULL LIMIT 1",
                    (email,), fetch="one")
                if row:
                    uid = int(row["user_id"])
                    logger.info(f"[MP Webhook] UID via pagamentos antigos: {uid}")
            except Exception:
                pass

        if not uid:
            logger.error(f"[MP Webhook] UID não encontrado — ext_ref='{ext_ref}' email='{email}'")
            # Notifica admin com todos os dados para ativação manual
            if telegram_app and _main_loop and _main_loop.is_running():
                asyncio.run_coroutine_threadsafe(
                    telegram_app.bot.send_message(
                        cfg.ID_ADMIN,
                        f"⚠️ <b>Pagamento SEM UID!</b>\n\n"
                        f"💰 Valor: R$ {valor:.2f}\n"
                        f"🔖 Payment ID: <code>{data_id}</code>\n"
                        f"📋 external_reference: <code>{ext_ref}</code>\n"
                        f"📧 Email: <code>{email}</code>\n\n"
                        f"🔍 <b>Para encontrar o UID:</b>\n"
                        f"Peça ao usuário que enviou o email acima para enviar /meuid no bot.\n\n"
                        f"✅ <b>Ative manualmente:</b>\n"
                        f"<code>/ativar [UID] 30</code>\n\n"
                        f"📌 Se o usuário tiver cadastrado email:\n"
                        f"Consulte no NeonDB:\n"
                        f"<code>SELECT id FROM emails WHERE email='{email}';</code>",
                        parse_mode="HTML"
                    ),
                    _main_loop
                )
            return jsonify({"status": "uid_not_found", "email": email}), 200

        # ── Idempotência ──────────────────────────────────────────
        if db.pgto_processado(order_id):
            logger.info(f"[MP Webhook] Já processado: {order_id}")
            return jsonify({"status": "already_processed"}), 200

        # ── Processa conforme status ──────────────────────────────
        STATUS_APROVADOS = {"approved", "authorized"}
        STATUS_CANCELADOS = {"cancelled", "refunded", "charged_back"}

        if status in STATUS_APROVADOS:
            dias       = PLANOS.get(plano, {}).get("dias", 30)
            venc       = db.ativar(uid, dias, plano, email)
            plano_nome = PLANOS.get(plano, {}).get("nome", "Mensal")

            if email:
                db.salvar_email(email, uid)

            db.log_pagamento(uid, order_id, "paid", valor, "mercadopago", email, plano)
            db.marcar_pgto(order_id)

            logger.info(f"[MP Webhook] ✅ uid={uid} ATIVADO até {venc} (R${valor:.2f})")

            # Notifica admin
            Notif.assinatura_sync(uid, plano, venc, valor, email, "mercadopago")

            # Notifica usuário
            if telegram_app and _main_loop and _main_loop.is_running():
                asyncio.run_coroutine_threadsafe(
                    telegram_app.bot.send_message(
                        uid,
                        f"🎉 <b>Pagamento confirmado!</b>\n\n"
                        f"📋 Plano: <b>{plano_nome}</b>\n"
                        f"💰 Valor: <b>R${valor:.2f}</b>\n"
                        f"📅 Válido até: <b>{venc}</b>\n\n"
                        f"Use /start para acessar todos os recursos! 🚀",
                        parse_mode="HTML",
                        reply_markup=InlineKeyboardMarkup([[
                            InlineKeyboardButton("🚀 Acessar o Bot", callback_data="main_menu")
                        ]])
                    ),
                    _main_loop
                )

            # Processa bônus de indicação
            ref_id = db.processar_referral(uid)
            if ref_id and telegram_app and _main_loop and _main_loop.is_running():
                asyncio.run_coroutine_threadsafe(
                    telegram_app.bot.send_message(
                        ref_id,
                        f"🎁 <b>Bônus de indicação!</b>\n\n"
                        f"+{cfg.BONUS_CONVIDANTE} dias adicionados à sua assinatura! 🎉",
                        parse_mode="HTML"
                    ),
                    _main_loop
                )

        elif status in STATUS_CANCELADOS:
            db.desativar(uid)
            db.log_pagamento(uid, order_id, status, 0, "mercadopago", email, plano)
            db.marcar_pgto(order_id)
            logger.info(f"[MP Webhook] ❌ uid={uid} DESATIVADO (status={status})")

            if telegram_app and _main_loop and _main_loop.is_running():
                asyncio.run_coroutine_threadsafe(
                    telegram_app.bot.send_message(
                        uid,
                        f"⚠️ <b>Assinatura cancelada.</b>\n\n"
                        f"Status: <b>{status}</b>\n\n"
                        f"Seu acesso ao bot foi suspenso.\n"
                        f"Use /start para renovar.",
                        parse_mode="HTML",
                        reply_markup=InlineKeyboardMarkup([[
                            InlineKeyboardButton(
                                "💳 Renovar Agora",
                                url=criar_link_pagamento(uid))
                        ]])
                    ),
                    _main_loop
                )
        else:
            # pending, in_process, etc — loga e aguarda
            logger.info(f"[MP Webhook] Status intermediário '{status}' — aguardando aprovação")

        return jsonify({"status": "ok", "uid": uid, "payment_status": status}), 200

    except Exception as e:
        logger.error(f"[MP Webhook] Exceção: {e}", exc_info=True)
        return jsonify({"status": "error", "detail": str(e)}), 500
    
    
@flask_app.route("/health", methods=["GET"])
def health(): return jsonify({"status":"ok","ts":datetime.now().isoformat()}), 200
    
    
def run_flask():
    for port in [5000, 5001, 5002, 8080]:
        try:
            flask_app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
            return
        except OSError:
            continue
    
    
    # ══════════════════════════════════════════════════════════════
    #  MAIN
    # ══════════════════════════════════════════════════════════════
async def post_init(app: Application):
    """Inicializa o bot, define _main_loop e inicia workers."""
    global _main_loop
    # get_running_loop() garante que pegamos o loop que está RODANDO agora
    _main_loop = asyncio.get_running_loop()

    # Reseta ultimo_auto_post de todos ao iniciar
    # Garante que o primeiro ciclo posta imediatamente
    try:
        db._exec("UPDATE assinantes SET ultimo_auto_post='' WHERE modo_auto=1")
        logger.info("✅ ultimo_auto_post resetado — primeiro ciclo será imediato")
    except Exception as e:
        logger.warning(f"Reset auto_post: {e}")

    # Registra comandos
    await app.bot.set_my_commands([
        BotCommand("start",           "Menu principal"),
        BotCommand("status",          "Status rápido da assinatura"),
        BotCommand("pagou",           "Ativar assinatura após pagamento"),
        BotCommand("email",           "Cadastrar email"),
        BotCommand("meuid",           "Ver seu ID"),
        BotCommand("ajuda",           "Central de ajuda"),
        BotCommand("list",            "Ver biblioteca de links"),
        BotCommand("produto",         "Postar produto: /produto <link>"),
        BotCommand("admin",           "Painel admin"),
        BotCommand("assinantes",      "Ver assinantes (admin)"),
        BotCommand("broadcast_teste", "Broadcast só para usuários em teste (admin)"),
        BotCommand("ativar",          "Ativar assinatura (admin)"),
        BotCommand("desativar",       "Desativar assinatura (admin)"),
        BotCommand("corrigir_ativos", "Reativar usuários desativados por engano (admin)"),
        BotCommand("broadcast",       "Enviar mensagem em massa (admin)"),
    ])

    # Inicia workers DEPOIS que _main_loop está definido e rodando
    bot          = app.bot
    auto_poster  = AutoPoster(bot)
    monitor_venc = MonitorVencimentos(bot)
    verif_assin  = VerificadorAssinaturas(bot)

    # AutoPoster e monitores em threads (são síncronos internamente)
    auto_poster.iniciar()
    monitor_venc.iniciar()
    verif_assin.iniciar()

    # Agendador roda no loop principal do Telegram (evita deadlock)
    scheduler = PostadorAgendado(bot)
    scheduler.iniciar()

    logger.info("🤖 Auto-poster | ⏰ Agendador | 🔔 Monitor | 🛡️ VerifAssin iniciados")
    logger.info("✅ Bot inicializado com sucesso!")
    
    
def main():
    global telegram_app

    # ── Telegram ──────────────────────────────────────────────
    telegram_app = (
        Application.builder()
        .token(cfg.TOKEN)
        .post_init(post_init)
        .build()
    )
    app = telegram_app

    # Handlers de comando
    app.add_handler(CommandHandler("start",     cmd_start))
    app.add_handler(CommandHandler("status",    cmd_status))
    app.add_handler(CommandHandler("pagou",     cmd_pagou))
    app.add_handler(CommandHandler("meuid",     cmd_meuid))
    app.add_handler(CommandHandler("email",     cmd_email))
    app.add_handler(CommandHandler("ajuda",     cmd_ajuda))
    app.add_handler(CommandHandler("list",      cmd_list))
    app.add_handler(CommandHandler("produto",   cmd_produto))
    app.add_handler(CommandHandler("admin",     cmd_admin))
    app.add_handler(CommandHandler("ativar",          cmd_admin_ativar))
    app.add_handler(CommandHandler("desativar",       cmd_admin_desativar))
    app.add_handler(CommandHandler("corrigir_ativos", cmd_corrigir_ativos))
    app.add_handler(CommandHandler("broadcast",        cmd_broadcast))
    app.add_handler(CommandHandler("broadcast_teste",  cmd_broadcast_teste))
    app.add_handler(CommandHandler("assinantes",  cmd_assinantes))

    # Handler de erros global — evita que o bot trave
    async def error_handler(update, context):
        err = str(context.error)
        if "Query is too old" in err or "timeout" in err.lower():
            return  # erros normais de timeout — ignora
        import traceback
        logger.error(f"[Bot] Erro: {err}")
        logger.error(traceback.format_exc())
    app.add_error_handler(error_handler)

    # Handlers de callback
    app.add_handler(CallbackQueryHandler(
        callback_admin,
        pattern="^adm_",
    ))
    app.add_handler(CallbackQueryHandler(callback))

    # Handler de texto / links / encaminhamentos
    app.add_handler(MessageHandler(
        filters.TEXT | filters.FORWARDED | filters.StatusUpdate.CHAT_SHARED,
        handle_text
    ))

    # ── Flask (webhooks) ──────────────────────────────────────
    threading.Thread(target=run_flask, daemon=True, name="Flask").start()
    logger.info("🌐 Flask rodando para webhooks")

    async def shutdown():
        """Limpa recursos antes de fechar."""
        if Extratores._ml:
            await Extratores._ml.fechar()
        logger.info("🛑 Recursos liberados")

    import atexit
    def _shutdown_sync():
        """Limpa recursos síncronos antes de fechar."""
        try:
            if Extratores._ml and hasattr(Extratores._ml, 'session'):
                # Fecha sessão aiohttp de forma segura se o loop ainda existir
                try:
                    loop = asyncio.get_event_loop()
                    if not loop.is_closed():
                        loop.run_until_complete(Extratores._ml.fechar())
                except Exception:
                    pass
        except Exception:
            pass
        logger.info("🛑 Recursos liberados")
    atexit.register(_shutdown_sync)
    
    # ── Run ────────────────────────────────────────────────────
    logger.info("🚀 Bot iniciado! Aguardando mensagens...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
