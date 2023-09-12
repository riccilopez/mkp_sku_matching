from modules.normalize_text import normalize_text
from modules.sku_matcher import get_confidence

def test_get_confidence():
    s1 = 'Agua Natural Nestle Pureza Vital botella 1 L 12 PIEZAS'
    s2 = "NESTLE PV 12x1000 ML Modelo"
    s1, s2 = normalize_text(s1), normalize_text(s2)
    assert get_confidence(s1, s2) == 1.0

    s1 = 'Brandy Domecq Don Pedro 200 ml Presentación'
    s2 = "Brandy DON PEDRO 200ml"
    s1, s2 = normalize_text(s1), normalize_text(s2)
    assert get_confidence(s1, s2) == 1.0

    s1 = 'Bebida Caribe Cooler Tinto 300 ml Presentación: Caja 12 Artículo(s)'
    s2 = "CARIBE COOLER TINTO 300 ML - 12 PZ"
    s1, s2 = normalize_text(s1), normalize_text(s2)
    assert get_confidence(s1, s2) == 1.0


if __name__ == "__main__":
    import pytest
    pytest.main()