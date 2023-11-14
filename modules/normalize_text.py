import re
import unicodedata
from collections.abc import Iterable
from pathlib import Path

# Function to normalize text
trans_tab = dict.fromkeys(map(ord, u'\u0301\u0308'), None)

def nltk_stopwords():
    path_file = (Path(__file__).parent / "./dictionary/nltk_stopwords_es.txt").resolve()
    with open(path_file, 'r') as f:
        stopwords = [word.rstrip() for word in f]
    return stopwords


def remove_accents(text: str, encode = 'macroman') -> str:
    '''A simple function to remove accent characters'''
    # TODO: check enconding in inputs
    #text =  text.encode('latin-1').decode('utf-8')
    #text =  text.encode(encode, 'ignore').decode('utf-8')
    text = unicodedata.normalize('NFKD', text)\
                .encode('ascii', 'ignore').decode("utf-8")
    return text

def remove_special_characters(text: str) -> str:
    '''A simple function to remove special characters'''
    text = re.sub(r'[(|)|-|–|-]', r' ', text)
    text = re.sub(r'&apos;', r'', text)
    text = re.sub(r'(\d+)\.(\d+)', r'\1#\2', text)
    text = re.sub(r'[·|•|\'|"|®|*|:|%]', r'', text)
    text = re.sub(r'\.', r' ', text)
    text = re.sub(r'(\d+)#(\d+)', r'\1.\2', text)
    return text

def remove_es_stopwords(text: str) -> str:
    '''A simple function to remove spanish stopwords'''
    _extra_stopwords = ['sabor']
    _base_stopwords  = nltk_stopwords() 
    _stopwords       = _base_stopwords + _extra_stopwords
    text = ' '.join([word for word in text.split(' ') 
                     if word not in _stopwords])
    return text

def remove_duplicated_tokens(text: str) -> str:
    '''A simple function to remove duplicated tokens'''
    words = text.split()
    text = " ".join(sorted(set(words), key = words.index))
    return text

def extract_units(s_clean: str) -> Iterable:
    '''Extracts the measure units from a cleaned string
    '''
    units = re.findall(r'\d*[.]?\d+[ml|lt|pz|g|oz|kg]+', s_clean)
    return units

def mil_units_simplification(text: str) -> str:
    """Simplifies """
    text = re.sub(r'(\d)\d{2}(g)', r'\g<1>00\2', text)
    text = re.sub(r'(\d)\d{1}(ml)', r'\g<1>0\2', text)
    return text

def homogenize_units(s: str) -> str:
    s = re.sub(r'(\d|\s|^)(p|zk|pieza|pzas|articulo|unidades|rollo|unid|c/u|cajetilla|unidad|und|pza|pzs|pk|botella|bulto|lata|laton|charola)[s|\(s\)]?(\s|$|/)', r'\1pz\3', s)
    s = re.sub(r'(^| )(pack|paquete|caja) (de|con) ([\d]+)', r'\1\4pz', s)
    s = re.sub(r'([\d*|\s])cig(\s|$)', r'\1pz\2', s)
    s = re.sub(r'(^| )and( |$)', r'\1&\2', s)
    # Misc
    s = re.sub(r'(^| )(etiqueta|label|pet|rep100%|tetra|display)( |$)', r'\1\3', s)
    #
    s = re.sub(r'presentacion', r'', s)
    s = re.sub(r'([\d|\s])ct(\s|$)', r'\1pz\2', s)
    s = re.sub(r'([\d]+)\s(pack)', r'\1pz', s)
    s = re.sub(r'(box|paquete|pack|cja|cj|caja|bolsa|bolsas|vaso)(\s|$)', r'', s)
    s = re.sub(r'(bebida( alcoholica)*)', r'', s)
    s = re.sub(r'(\s|[\d]+)(mls|mililitros|m)(/|-|\s|$)', r'\1ml\3', s)
    s = re.sub(r'([\d|\s])(lts|litros|litro|l)(/|-|\s|$)', r'\1lt\3', s)
    s = re.sub(r'(onzas|onza|oza|onz)(/|-|\s|$)', r'oz', s)
    s = re.sub(r'(\s|[\d]+)(g|gramos|gs|gr|grs|gramo)(/|-|\s|$)', r'\1g\3', s)
    s = re.sub(r'([\d]+)\s(ml|pz|g|kg|lt|oz)', r'\1\2', s)
    s = re.sub(r'([\d]+)[\s]+[\+]([\d]+)ml', r'\1ml \2ml', s)
    s = re.sub(r'(\s|^)2x1(\s|$)', r'\1twoxone\2', s)
    s = re.sub(r'([\d]{3,}$)', r'\1ml', s)
    s = re.sub(r'(1000ml|\slt)(\s|$)', r' 1lt\2', s) # ml to lt
    s = re.sub(r'(6|12|24|36)\s(\d{1,4}[ml|g])', r'\g<1>pz \g<2>', s)
    s = re.sub(r'([\d]+)(/|x| x )', r'\1pz ', s)
    s = re.sub(r'(\s|1|^)pz(\s|$)', r' ', s)
    s = re.sub(r'(^| )[a-z]( |$)', r'\1\2', s)
    s = re.sub(r'/', r' ', s)
    return s

def abbreviations_correction(s: str) -> str:
    # Remove plural
    s = re.sub(r"([aeioumn])s($|\s)", r'\1\2', s)
    # countries
    s = re.sub(r"(mexico)", r'', s)
    # TODO: convert to a dictionary to improve readability
    s = re.sub(r'(\s|^)(suero rehidratante)(\s|$)', r'\1\3', s)
    s = re.sub(r'(\s|^)(suero)(\s|$)', r'\1\3', s)
    s = re.sub(r'(\s|^)(hidratante)(\s|$)', r'\1\3', s)
    s = re.sub(r'(\s|^)(deslactosada)(\s|$)', r'\1delac\3', s)
    s = re.sub(r'(\s|^)(adulto)(\s|$)', r'\1adlt\3', s)
    s = re.sub(r'(\s|^)(energ[a-z]+)(\s|$)', r'\1\3', s)
    s = re.sub(r'(\s|^)(cigarro[s]*|cig)(\s|$)', r'\1\3', s)
    s = re.sub(r'(\s|^)tequila(\s|$)', r'\1teq\2', s) # minimiza el peso de descripciones
    s = re.sub(r'(\s|^)whiskey(\s|$)', r'\1whisky\2', s)
    s = re.sub(r'(\s|^)brandy(\s|$)', r'\1brdy\2', s)
    s = re.sub(r'(\s|^)aceite(\s|$)', r'\1ace\2', s)
    s = re.sub(r'(\s|^)ace soya(\s|$)', r'\1ace\2', s)
    s = re.sub(r'(\s|^)promo(\s|$)', r'\1\2', s)
    s = re.sub(r'(\s|^)fresh(\s|$)', r'\1fsh\2', s)
    s = re.sub(r'(\s|^)tradicional(\s|$)', r'\1trad\2', s)
    s = re.sub(r'(\s|^)estandar|standard(\s|$)', r'\1esd\2', s)
    s = re.sub(r'(\s|^)original(\s|$)', r'\1\2', s)
    s = re.sub(r'(\s|^)papel higienico(\s|$)', r'\1\2', s)
    s = re.sub(r'(\s|^)roj[a|o](\s|$)', r'\1red\2', s)
    s = re.sub(r'(\s|^)golden(\s|$)', r'\1gold\2', s)
    s = re.sub(r'(\s|^)negr[a|o](\s|$)', r'\1black\2', s)
    s = re.sub(r'(\s|^)reposado(\s|$)', r'\1rep\2', s)
    s = re.sub(r'(\s|^)especial(\s|$)', r'\1esp\2', s)
    s = re.sub(r'(\s|^)edicion(\s|$)', r'\1ed\2', s)
    s = re.sub(r'(\s|^)(\d*) ano(\s|$)', r'\1\2\2ano\3', s)
    s = re.sub(r'(\s|^)cristalino(\s|$)', r'\1cristal\2', s)
    s = re.sub(r'(\s|^)pocket(\s|$)', r'\1\2', s)
    # 
    #s = re.sub(r'(\s|^)licor cana(\s|$)', r'\1licorcana\2', s)
    s = re.sub(r'(\s|^)(licor|destilado)(\sagave)*(\s|$)', r'\1\4', s)
    s = re.sub(r'(\s|^)(alim[\.]*)(\s|$)', r'\1amilento\3', s)
    # homologacion de marcas
    s = re.sub(r'modelo$', r'', s)
    s = re.sub(r'([\s]*)promo(\s|$)', r'\1\2', s)
    s = re.sub(r'([\s]*)pouch[e]?(\s|$)', r'\1pch\2', s)
    s = re.sub(r'([\s]*)jugo(\s|$)', r'\1\2', s)
    s = re.sub(r'([\s]*)promocion(\s|$)', r'\1\2', s)
    s = re.sub(r'(\s|^)sopa (instantanea)', r'sopa', s)
    s = re.sub(r'(\s|^)sopa (ramen)', r'sopa', s)
    s = re.sub(r'(\s|^)(chile)* habanero(\s|$)', r'\1habanero\3', s) 
    s = re.sub(r'(\s|^)(chile)* piquin(\s|$)', r'\1piquin\3', s)
    s = re.sub(r'(\s|^)(quereta[a-z]+ )?verde valle( quereta[a-z]+)?(\s|$)', r'\1verdevalle\4', s)
    # hard liquor
    s = re.sub(r'(\s|^)agua natural(\s|$)', r'\1\2', s)
    s = re.sub(r'(\s|^)refresco(\s|$)', r'\1\2', s)
    s = re.sub(r'(\s|^)producto lacteo(\s|$)', r'\1leche\2', s)
    # Brand normalization
    if 'maruchan' in s:
        s = re.sub(r'maruchan', r'sopa maruchan ramen instantanea 64g', s)
        s = re.sub(r'picante|chile', 'piquin', s) 
        s = re.sub(r'carne re', 're', s)
        s = re.sub(r'camaron?\s*piquin', 'camaron piquin', s)  
    if 'skyy' in s:
        s = re.sub(r'(\s|^)mezcla|vodka|blue|original(\s|$)', r'\1\2', s)
        s = re.sub(r'(\s|^)skyy(\s|$)', r'\1vodkaskyyblue\2', s)
        # Sabores
        s = re.sub(r'appletini(:?\smanzana)?(:?\sverde)?', r'ap', s)
        s = re.sub(r'275(\s|$)', r' 275ml\1', s)
        s = re.sub(r'cosmo(:?\sarandano)?', r'cs', s)
        s = re.sub(r'citru', r'', s)
    s = re.sub(r'camaron?\s*habanero', 'camaron piquin', s)
    s = re.sub(r'(\s|^)rb(\s|$)', r'\1redbull\2', s)
    s = re.sub(r'(\s|^)red bull(\s|$)', r'\1redbull\2', s)
    s = re.sub(r'(\s|^)vive\s?100[pz]*(\s|$)', r'\1vive100\2', s)
    s = re.sub(r'(\s|^)lol tun(\s|$)', r'\1loltun\2', s)
    s = re.sub(r'(\s|^)vogue 600hoja(\s|$)', r'\1vogue 600 hoja\2', s)
    #s = re.sub(r'(\s|^)(domecq )*don pedro(\s|$)', r'\1donpedro\3', s)
    s = re.sub(r'(\s|^)don pedro(\s|$)', r'\1donpedro\2', s)
    s = re.sub(r'(\s|^)(bry\s)?(domecq)?\sdonpedro(\s|$)', r'\1\2donpedro\4', s)
    s = re.sub(r'(\s|^)bacardi carta blanca(\s|$)', r'\1bacardi blanco\2', s)
    s = re.sub(r'(\s|^)(bry )*azteca oro(\s|$)', r'\1aztecaoro\3', s)
    s = re.sub(r'passport scot[c]?h', r'passport', s)
    s = re.sub(r'(\s|^)sauza hacienda(\s|$)', r'\1sauzahacienda\2', s)
    s = re.sub(r'(\s|^)campo azul(\s|$)', r'\1campoazul\2', s)
    s = re.sub(r'(\s|^)johnne(\s|$)', r'\1johnnie\2', s)
    s = re.sub(r'(\s|^)buchana(\s|$)', r'\1buchanan\2', s)
    s = re.sub(r'(\s|^)cava de oro(\s|$)', r'\1cavadeoro\2', s)
    s = re.sub(r'(\s|^)don julio(\s|$)', r'\1donjulio\2', s)
    s = re.sub(r'(\s|^)rancho escondido(\s|$)', r'\1ranchoescondido\2', s)
    s = re.sub(r'(\s|^)jose cuervo(\s|$)', r'\1cuervo\2', s)
    #s = re.sub(r'(\s|^)[whisky ]*johnnie walker(\s|$)', r'\1johnnie walker\2', s)
    s = re.sub(r'(\s|^)gran centenar[i]*o(\s|$)', r'\1grancentenario\2', s)
    s = re.sub(r'(\s|^)teq 1800(\s|$)', r'\1cuervo 1800\2', s)
    s = re.sub(r'(\s|^)(teq|destilado|licor)\scompadre(\s|$)', r'\1compadre\3', s)
    s = re.sub(r'(\s|^)nestle pureza vital(\s|$)', r'\1npv\2', s)
    s = re.sub(r'(\s|^)nestle pv(\s|$)', r'\1npv\2', s)
    s = re.sub(r'(\s|^)lechera(\s|$)', r'\1lechera nestle condensada\2', s)
    s = re.sub(r'(\s|^)pepsi cola(\s|$)', r'\1pepsi\2', s)
    s = re.sub(r'(\s|^)vitaloe original(\s|$)', r'\1vitaloe\2', s)
    s = re.sub(r'(\s|^)(vel rosita)(\s|$)', r'\1velrosita\3', s)
    s = re.sub(r'(\s|^)caribe cooler(\s|$)', r'\1caribecooler\2', s)
    s = re.sub(r'(\s|^)(sta[\.]*)(\s|$)', r'\1santa\3', s)
    s = re.sub(r'(\s|^)(agua )*(natural )*(santa maria)(\s|$)', r'\1santamaria\5', s)
    s = re.sub(r'(\s|^)(agua )*(natural )*(san pellegrino)(\s|$)', r'\1san pellegrino\5', s)
    # brand weight
    s = remove_duplicated_tokens(s)
    norm_brands = ['caribecooler', 'velrosita', 'lala', 'grancentenario',
                   'mezcalito', 'alpura', 'campoazul', 'cabrito', 'aztecaoro', 'bacardi',
                   'jimador', 'donjulio', 'npv', 'pepse', 'vitaloe', 'vodkaskyyblue', 
                   'smirnoff', 'cuervo', 'jumex','presidente', 'sauzahacienda', 'boing',
                   'redbull', 'santamaria', 'electrolit', 'loltun']
    # iterate over corrected brands 
    for brand in norm_brands:
        # determinate the weight of the brand accroding to its length name
        len_brand = len(brand)
        if len_brand <= 5:
            expanded_brand = 4*brand
        elif len_brand <= 8:
            expanded_brand = 3*brand
        else: expanded_brand = 2*brand
        s = re.sub(r'(\s|^)(' + brand + ')(\s|$)', r'\1' + expanded_brand + '\3', s)

    # s = re.sub(r'(\s|^)(caribecooler)(\s|$)', r'\1\2\2\2\3', s) 
    # s = re.sub(r'(\s|^)(velrosita)(\s|$)', r'\1\2\2\2\3', s)
    # s = re.sub(r'(\s|^)(lala)(\s|$)', r'\1\2\2\2\2\3', s)
    # s = re.sub(r'(\s|^)(grancentenario)(\s|$)', r'\1\2\2\2\3', s)
    # s = re.sub(r'(\s|^)(mezcalito)(\s|$)', r'\1\2\2\2\3', s)
    # s = re.sub(r'(\s|^)(alpura)(\s|$)', r'\1\2\2\2\3', s)
    # s = re.sub(r'(\s|^)(campoazul)(\s|$)', r'\1\\22\2\3', s)
    # s = re.sub(r'(\s|^)(cabrito)(\s|$)', r'\1\2\2\2\3', s)
    # s = re.sub(r'(\s|^)(jimador)(\s|$)', r'\1\2\2\2\3', s)
    # s = re.sub(r'(\s|^)(donjulio)(\s|$)', r'\1\2\2\2\3', s)
    # 
    
    return s

def normalize_text(text: str, encode = 'macroman') -> str:
    text = str(text)
    # Lower case 
    text = text.strip().lower()
    # Special characters
    text = remove_special_characters(text)
    # remove tildes
    text = remove_accents(text, encode = encode)
    # Multiple blanks
    text = ' '.join(text.split())
    # homogenize_units
    text = homogenize_units(text)
    # Volume/weight values simplification
    text = mil_units_simplification(text)
    # remove stop words
    text = remove_es_stopwords(text)
    # Specific brand correction
    text = abbreviations_correction(text)
    # Multiple blanks
    text = ' '.join(text.split())
    # duplicated tokens
    text = remove_duplicated_tokens(text)
    return text