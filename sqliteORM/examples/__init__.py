from .. import rows, db
from ..types import INTEGER, TEXT
from os import path

from .. import logger_builder
logger = logger_builder.build_logger(__name__)

DB = db.DB(
    path=path.join(path.dirname(__file__), "database.db"),
    debug=True
)

class Personns(db.DBTable):
    @classmethod
    def create(cls):
        Personns.add_row(rows.DBRow("id", INTEGER, True, primary=True))
        Personns.add_row(rows.DBRow("firstName", TEXT(30), nullable=False))
        Personns.add_row(rows.DBRow("lastName", TEXT(50), nullable=False))
        Personns.add_row(rows.DBRow("bornYear", INTEGER, nullable=False))
        Personns.add_row(rows.DBRow("email", TEXT(100), nullable=True, unique=True))
        Personns.add_row(rows.Relations(
            "inscriptions", 
            Inscriptions, match_with={Inscriptions.get_row("personn_id"): cls.get_row("id")}
        ))
        

class Inscriptions(db.DBTable):
    @classmethod
    def create(cls):
        
        Inscriptions.add_row(rows.DBRow.build_id_row())
        Inscriptions.add_row(rows.DBRow("personn_id", INTEGER, nullable=False, foreign_key=Personns.get_row("id")))
        Inscriptions.add_row(rows.DBRow("site_id", INTEGER, nullable=False, foreign_key=Website.get_row("id")))


class Website(db.DBTable):
    @classmethod
    def create(cls) -> None:
        Website.add_row(rows.DBRow.build_id_row())
        Website.add_row(rows.DBRow("name", TEXT(40), nullable=False, unique=True))
        Website.add_row(rows.Relations(
            "inscriptions",
            Inscriptions, match_with={Inscriptions.get_row("site_id"): cls.get_row("id")}
        ))
        
DB.add_table(Personns)
DB.add_table(Inscriptions)
DB.add_table(Website)

DB.create_tables()

arthur = Personns.get_by(firstName="Arthur", lastName="WOELFEL", email="woelfel.arthur@gmail.com", bornYear=2007)
pierre = Personns.get_by(firstName="Pierre", lastName="MAILLE", email="pmaille75@gmail.com", bornYear=2007)


amazon = Website.get_by(name="amazon")
leboncoin = Website.get_by(name="Le Bon Coin")
troll2j = Website.get_by(name="Troll2jeux")
DB.commit("Ajout de personnes et de sites")

a_amazon = Inscriptions(personn_id=arthur.id, site_id=amazon.id)
p_amazon = Inscriptions(personn_id=pierre.id, site_id=amazon.id)
a_t2j = Inscriptions(personn_id=arthur.id, site_id=troll2j.id)
a_lbc = Inscriptions(personn_id=arthur.id, site_id=leboncoin.id)

DB.commit("Ajout d'inscriptions")