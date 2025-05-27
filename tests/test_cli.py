from pathlib import Path
from bblocks.datacommons_tools.cli import main


def test_csv2mcf(tmp_path: Path) -> None:
    csv = tmp_path / "sv.csv"
    csv.write_text("Node,name,typeOf\nsv1,SV1,dcid:StatisticalVariable\n")
    out_mcf = tmp_path / "out.mcf"
    # Run cli
    exit_code = main(["csv2mcf", str(csv), str(out_mcf)])
    assert exit_code == 0
    assert out_mcf.exists()
    content = out_mcf.read_text()
    assert "Node: sv1" in content
    assert 'name: "SV1"' in content
