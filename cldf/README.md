<a name="ds-genericmetadatajson"> </a>

# Generic Australian Message Stick Database

**CLDF Metadata**: [Generic-metadata.json](./Generic-metadata.json)

property | value
 --- | ---
[dc:bibliographicCitation](http://purl.org/dc/terms/bibliographicCitation) | Kelly, Piers, Junran Lei, Hans-Jörg Bibiko, and Lorina Barker. 2024. "AMSD: The Australian Message Stick Database." PLOS One 19 (4): e0299712. doi: https://doi.org/10.1371/journal.pone.0299712
[dc:conformsTo](http://purl.org/dc/terms/conformsTo) | [CLDF Generic](http://cldf.clld.org/v1.0/terms.rdf#Generic)
[dc:identifier](http://purl.org/dc/terms/identifier) | https://amsd.clld.org/
[dcat:accessURL](http://www.w3.org/ns/dcat#accessURL) | https://github.com/clld/amsd-data
[prov:wasDerivedFrom](http://www.w3.org/ns/prov#wasDerivedFrom) | <ol><li><a href="https://github.com/clld/amsd-data/tree/v2026.07">clld/amsd-data  v2026.07</a></li><li><a href="https://github.com/glottolog/glottolog/tree/v5.3">Glottolog  v5.3</a></li></ol>
[prov:wasGeneratedBy](http://www.w3.org/ns/prov#wasGeneratedBy) | <ol><li><strong>python</strong>: 3.12.3</li><li><strong>python-packages</strong>: <a href="./requirements.txt">requirements.txt</a></li></ol>
[rdf:ID](http://www.w3.org/1999/02/22-rdf-syntax-ns#ID) | amsd
[rdf:type](http://www.w3.org/1999/02/22-rdf-syntax-ns#type) | http://www.w3.org/ns/dcat#Distribution


## <a name="table-languagescsv"></a>Table [languages.csv](./languages.csv)

Linguistic areas

property | value
 --- | ---
[dc:conformsTo](http://purl.org/dc/terms/conformsTo) | [CLDF LanguageTable](http://cldf.clld.org/v1.0/terms.rdf#LanguageTable)
[dc:extent](http://purl.org/dc/terms/extent) | 159


### Columns

Name/Property | Datatype | Description
 --- | --- | --- 
[ID](http://cldf.clld.org/v1.0/terms.rdf#id) | `string`<br>Regex: `[a-zA-Z0-9_\-]+` | Primary key
[Name](http://cldf.clld.org/v1.0/terms.rdf#name) | `string` | 
[Macroarea](http://cldf.clld.org/v1.0/terms.rdf#macroarea) | `string` | 
[Latitude](http://cldf.clld.org/v1.0/terms.rdf#latitude) | `decimal`<br>&ge; -90<br>&le; 90 | 
[Longitude](http://cldf.clld.org/v1.0/terms.rdf#longitude) | `decimal`<br>&ge; -180<br>&le; 180 | 
[Glottocode](http://cldf.clld.org/v1.0/terms.rdf#glottocode) | `string`<br>Regex: `[a-z0-9]{4}[1-9][0-9]{3}` | 
[ISO639P3code](http://cldf.clld.org/v1.0/terms.rdf#iso639P3code) | `string`<br>Regex: `[a-z]{3}` | 
`Austlang_Code` | `string` | 

## <a name="table-mediacsv"></a>Table [media.csv](./media.csv)

property | value
 --- | ---
[dc:conformsTo](http://purl.org/dc/terms/conformsTo) | [CLDF MediaTable](http://cldf.clld.org/v1.0/terms.rdf#MediaTable)
[dc:extent](http://purl.org/dc/terms/extent) | 10472


### Columns

Name/Property | Datatype | Description
 --- | --- | --- 
[ID](http://cldf.clld.org/v1.0/terms.rdf#id) | `string`<br>Regex: `[a-zA-Z0-9_\-]+` | Primary key
[Name](http://cldf.clld.org/v1.0/terms.rdf#name) | `string` | 
[Description](http://cldf.clld.org/v1.0/terms.rdf#description) | `string` | 
[Media_Type](http://cldf.clld.org/v1.0/terms.rdf#mediaType) | `string`<br>Regex: `[^/]+/.+` | 
[Download_URL](http://cldf.clld.org/v1.0/terms.rdf#downloadUrl) | `anyURI` | 
[Path_In_Zip](http://cldf.clld.org/v1.0/terms.rdf#pathInZip) | `string` | 

## <a name="table-contributionscsv"></a>Table [contributions.csv](./contributions.csv)

The AMSD has the strict structure of one discrete item per database entry. But within each
entry, a single item may have multiple sources informing it and more than one representation,
for example, an official museum photograph, a hand-drawn sketch in a notebook, an illustration
in a published article, etc. The core item types are labelled as follows, with counts accurate
at the time of publication:
- message stick in a collection (N = 1197),
- message stick in a private collection (N = 70),
- message stick from a private sale (N = 49),
- image of a message stick (artefact missing) (N = 170),
- footage of a message stick (N = 6) and
- image of a message stick and messenger (N = 5).

Three additional item types are associated with indirect sources evidence for message stick use.
These are:
- positive text reference (N = 19) referring to an observation of message stick use in a
particular time and place recorded in an archive,
- lexical item (N = 10) meaning an Indigenous term for message stick from an identifiable
Australian language, and
- message stick accessory (N = 4) referring to paraphernalia connected to a message stick such
as cleft carrying sticks.

The final two item types are
- negative text reference (N = 5) recording an archival observation that no message sticks are
used by a particular group or in a particular territory at a specific time in history, and
- fictional message stick (N = 22) for imaginative or artistic representations of message
sticks that are not known to have existed but which may have a bearing on the cultural
history of these objects.

Thus, in terms of plotting the distribution of message sticks, the final two item types must be
excluded from raw counts, although negative text reference may be used to identify historical
absences. These item types are designed to capture the range of data types that contribute to
the dataset as a whole, but may overlap.

property | value
 --- | ---
[dc:conformsTo](http://purl.org/dc/terms/conformsTo) | [CLDF ContributionTable](http://cldf.clld.org/v1.0/terms.rdf#ContributionTable)
[dc:extent](http://purl.org/dc/terms/extent) | 2152


### Columns

Name/Property | Datatype | Description
 --- | --- | --- 
[ID](http://cldf.clld.org/v1.0/terms.rdf#id) | `string`<br>Regex: `[a-zA-Z0-9_\-]+` | Primary key
[Name](http://cldf.clld.org/v1.0/terms.rdf#name) | `string` | 
[Description](http://cldf.clld.org/v1.0/terms.rdf#description) | `string` | 
[Contributor](http://cldf.clld.org/v1.0/terms.rdf#contributor) | `string` | 
[Citation](http://cldf.clld.org/v1.0/terms.rdf#citation) | `string` | 
`Item_Type` | `string`<br>Regex: `message\ stick\ found\ in\ situ\|message\ stick\ in\ a\ collection\|message\ stick\ in\ a\ private\ collection\|message\ stick\ from\ a\ private\ sale\|image\ of\ a\ message\ stick\ \(artefact\ missing\)\|footage\ of\ a\ message\ stick\|image\ of\ a\ message\ stick\ and\ messenger\|image\ of\ a\ message\ stick\ accessory\ and\ messenger\|text\ reference\|positive\ text\ reference\|lexical\ item\|message\ stick\ accessory\|negative\ text\ reference\|fictional\ message\ stick` | See above
`Item_Subtype` | `string` | 
`State_Territory` | list of `string`<br>Regex: `New\ South\ Wales\|Victoria\|Northern\ Territory\|Western\ Australia\|South\ Australia\|Queensland` (separated by ` + `) | 
`Cultural_Region` | `string` | 
`Motifs` | `string` | 
`Motif_Transcription` | `string` | 
`Keywords` | list of `string` (separated by ` + `) | 
`Object_Creator` | `string` | 
`Date_Created` | `int` | Year of creation. See also Note_Date_Created
`Note_Date_Created` | `string` | 
[Linguistic_Areas](http://cldf.clld.org/v1.0/terms.rdf#languageReference) | list of `string` (separated by ` `) | References [languages.csv::ID](#table-languagescsv)
`Note_Linguistic_Areas` | `string` | 
`Stick_Term` | `string` | 
`Message` | `string` | 
`Dimensions` | list of `string` (separated by ` `) | Dimensions of the message stick in mm.
`Note_Dimensions` | `string` | A note specifying how to interpret dimensions.
`Material` | list of `string`<br>Regex: `wood\ plant\|cotton\ yarn\ plant\|string\ binding\|kaolin\ clay\|gidyea\ \[acacia\ homalophylla\];\ wood\ plant\|pigment\|cuttibundi\|quinine\|peruvian\ bark\|ironbark\|white\ gum\|pine\ wood\|bean\-tree\ wood\|wood\|wood\ \("grey"\ gum\)\|leichhardt\ pine\ wood\|animal\ fur\ skin\|string\|bark\|fibre\ string\|resin\ \-\ non\ specific\|resin\|feather\|rope\|ochre\|wattle\ wood\ \(\?\)\|mimosa\ wood\ \(\?\)\|emu\ fat\|charcoal\|corkwood\|soil\|bamboo\|sandalwood\|hardwood\|brown\ wood\|pittosporum\ wood\|bone\|plant\ fiber\|gum\|animal\ dung\|textile\|ana\-jalhout\|wood\ \(ana\-jal\)\|myall\ wood\ \(acacia\ harpophylla\)\|pigment\?\|milk\ wood\|paint\|myall\ wood\|\(acacia\ harpophylla\)\|wattle\|natural\ pigments\ \(ochre\)\|paper\|ink\|ochre\ pigment\|mungal\ wood\|soft\ wood\|carved\ wood\|feathers\|ochres\ on\ ironwood\|ochres\ on\ wood\|plant\ fibre\ string\|fibre\ cords\|ochres\|ink\ on\ wood\|wood\ with\ color\|with\ color\|adhesive\ \-\ non\ specific\|red\ pigment\|reddish\-brown\ iron\ wood\|wood;\ pigment\|bamboo\ \(according\ to\ the\ accession\ registers:\ "prior\ material:\ wood"\)\|wood;\ resin\|wood;\ pigment;\ rockets\ blue\|wood;\ pigment;\ stone;\ resin\|wood\ and\ pigment\|bone\ \(according\ to\ icm\ online\ catalogue\)\|ocher\|plant\ fibers\|wood;\ ocher\|wood;\ dye;\ pigment\|wood;\ color\ pigment\|white\ paper\|wood;\ putty;\ organic\|wood;\ unknown\ material\|wood;\ pigment;\ binder\|organic;\|blackwood\ \(acacia\ melanoxylon\)\|hair` (separated by ` + `) | 
`Technique` | list of `string`<br>Regex: `incised\|carved\|notched\|bound\|incised\ \ carved\|painted\ \(blue\)\|the\ work\ has\ been\ accomplished\ with\ a\ marsupial\ incisor\|and\ is\ extremely\ faint\ in\ places\.\|raddled\ with\ a\ dark\-red\ ochre\|ochred\|twisted\|pigmented\|pokerwork\|engraved\|pyro\-engraved\|red\-colored\|carving\|painted\|painted\ \(red\)\|incised\ decoration\|inlaid\ with\ pigments\|incised\ decor\|depths\ with\ pigment\|carved\ and\ colored\|berndt\ card\ file\ under\ 'condition/preservation':\ "sprayed\ 1971"\|etching\|burnt\|carved\ and\ pokerwork\|etched\|pyro\-engraved\ \(the\ term\ refers\ to\ a\ technique\ used\ to\ decorate\ wood\ or\ other\ materials\ with\ burn\ marks\ created\ with\ a\ heated\ object\ used\ to\ engrave\ or\ incise\ the\ surface\ of\ the\ object\.\)\|curved\|scratched\|rubbed\|wood\ carving;\ painted\|cut;\ colored\|cut\|cut;\ painted\|carved\ and\ painted\|carved\ and\ engraved\|carve\|etch\|kerbschnitzerei\ \(google\ translate:\ chip\ carving\)\|schnitzerei;\ ritzung\ \(google\ translate:\ carving;\ incision\)\|ritzung\ \(google\ translate:\ engraving\)\|not\ specified\|schnitzerei;\ brandmalerei\ \(google\ translate:\ carving;\ pyrography\)\|schnitzerei\ \(google\ translate:\ carving\)\|shaped\|notched\ and\ incised\|shaped\ and\ incised\|unknown` (separated by ` + `) | 
`Source_Citation` | list of `string` (separated by ` + `) | 
`Source_Type` | list of `string`<br>Regex: `book\|journal\ article\|book\ chapter\|museum\ collection\|book\ article\|article\|text\ source\|newspaper\ article\|unpublished\ manuscript\|sale\ item,\ ebay\|private\ sale\|sale\ item,\ lawsons\ auctioneers\|sale\ item,\ www\.carters\.com\.au\|archive\|cultural\ institution\|book\ source\|ethnographic\ collection` (separated by ` + `) | 
`Year_Collected` | `integer` | 
`Note_Date_Collected` | `string` | 
`Holder_File` | `string` | 
`Holder_Object_ID` | `string` | 
`Collector` | `string` | 
`Place_Collected` | `string` | 
`File_Copyright` | `string` | 
[Latitude](http://cldf.clld.org/v1.0/terms.rdf#latitude) | `decimal`<br>&ge; -90<br>&le; 90 | 
[Longitude](http://cldf.clld.org/v1.0/terms.rdf#longitude) | `decimal`<br>&ge; -180<br>&le; 180 | 
`Note_Coordinates` | `string` | 
`URL_Institution` | `anyURI` | 
`Source_URLs` | list of `anyURI` (separated by ` `) | 
`Related` | `string` | References [related.csv::ID](#table-relatedcsv)
[Note](http://cldf.clld.org/v1.0/terms.rdf#comment) | `string` | 
`Semantic_Domains` | list of `string`<br>Valid choices:<br> `sd_journey` `sd_time` `sd_request_help` `sd_person_sender` `sd_person_recipient` `sd_request_invitation_ceremony` `sd_ceremony` `sd_person_group` `sd_person_woman_wife` `sd_request_invitation` `sd_illness` `sd_person` `sd_reminder` `sd_person_singer` `sd_person_woman` `sd_person_dancer` `sd_person_man_elder` `sd_person_man` `sd_ceremony_law` `sd_skin` `sd_person_boy` `sd_clan` `sd_activity_hunting` `sd_fence` `sd_animal_emu` `sd_animal_wallaby` `sd_acculturation` `sd_game` `sd_urgency` `sd_death` `sd_place` `sd_animal_ibis` `sd_place_river` `sd_time_moon` `sd_place_lawground` `sd_journey_route` `sd_activity_stay` `sd_activity_accompany` `sd_activity_travel` `sd_request_item` `sd_item_handkerchief` `sd_item_luxury` `sd_request` `sd_track` `sd_place_camp` `sd_ceremony_law_women` `sd_body_vulva` `sd_request_woman` `sd_person_woman_girl` `sd_person_woman_widow` `sd_ceremony_showfight` `sd_weapon_woomera` `sd_weapon_spear` `sd_payment` `sd_person_brother` `sd_person_sister` `sd_person_police` `sd_marriage` `sd_item_cloth` `sd_item_trousers` `sd_item_singlet` `sd_violence` `sd_person_mother` `sd_person_grandmother` `sd_weapon_boomerang` `sd_person_messenger` `sd_item_headband` `sd_person_child` `sd_person_father` `sd_request_invitation_ceremony_law` `sd_warning` `sd_person_enemy` `sd_time_wetseason` `sd_meeting` `to_exhibit` `sd_place_waterhole` `sd_place_dam` `sd_place_mission` `sd_number_1` `sd_food_wheatporridge` `sd_place_ground` `sd_gate` `sd_grass` `sd_animal_sheep` `sd_activity_takeperson` `sd_time_tomorrow` `sd_bridge` `sd_war` `sd_number` `sd_execution` `sd_trap` `sd_place_meetingplace` `sd_hill_sandhill` `sd_creek_sandycreek` `sd_place_country` `sd_item_pituri` `sd_request_item_pituri` `sd_activity_initiate` `sd_item` `sd_item_bullroarer` `sd_activity_markwithochre` `sd_person_son` `sd_request_invitation_ceremony_dance` `sd_place_dancingground` `sd_activity_walking` `sd_activity_war` `sd_tree` `sd_animal_grub` `sd_animal_goanna` `sd_time_star` `sd_direction_west` `sd_plant_tree` `sd_place_darwin` `sd_place_australianmainland` `sd_place_aspleystraits` `sd_person_group_melvilleislanders` `sd_person_group_bathurstislanders` `sd_place_walcottinlet` `sd_place_creek` `sd_person_group_language` `sd_activity_meeting` `sd_place_confluence` `sd_person_group_recipients` `sd_place_house` `sd_place_gibbriver` `sd_place_station` `sd_activity_kill` `sd_person_dead` `sd_news` `sd_event` `sd_place_hills` `sd_ceremony_funeral` `sd_request_alliance` `sd_body_penis` `sd_body_beard` `sd_mountain` `sd_item_blanket` `sd_drought` `sd_mosquito` `sd_fly` `sd_activity_ceremony` `sd_place_katherineriver` `sd_place_bush` `purpose_response` `purpose_request` `sd_activity_marriage` `sd_person_man_son` `sd_person_man_father` `sd_person_female_girl` `sd_item_tobacco` `purpose_news` `sd_place_arnhembay` `sd_place_yirrkala` `sd_injury` `sd_item_djudapole` `sd_ceremony_nara` `sd_sender_boy` `sd_messenger_white` `sd_place_dalywaters` `sd_activity_cuthair` `sd_activity_sendhair` `sd_recipient_woman` `sd_ceremony_setfight` (separated by ` `) | 
`Data_Entry` | list of `string`<br>Regex: `Piers\ Kelly\|Alexandra\ Roginski\|Julia\ Bespamyatnykh\|Olena\ Tykhostup\|Lorina\ Barker\|Nitzan\ Rotman\|11\.02\.2025\ PK:\ Wrote\ to\ seller\ requesting\ more\ information\|On\ same\ date,\ seller\ replied\ "\|Thank\ you\ for\ reaching\ out\.\|There\ is\ no\ documentation\ we\ can\ provide\.\ We\ can\ advise\ the\ vendor\ of\ this\ item\ provided\ this\ description\.\ Although\ we\ cannot\ reveal\ who\ they\ are,\ they\ are\ a\ well\-established\ and\ long\-standing\ collector\.\ We\ then\ had\ the\ entirety\ of\ this\ auction\ reviewed\ by\ an\ independent\ anthropologist\ who\ has\ confirmed\ all\ descriptions\."\|16\.11\.2025\ PK:\ Wrote\ to\ Torquay\ Museum:\ "Dear\ Torquay\ Museum,\|I\ am\ the\ lead\ researcher\ on\ the\ Australian\ Message\ Stick\ Project\ which\ aims\ to\ compile\ a\ global\ inventory\ of\ message\ sticks\ in\ world\ collections\.\|A\ search\ of\ your\ database\ has\ brought\ up\ two\ items,\ but\ there\ is\ a\ third\ mentioned\ in\ this\ book:\|Sculthorpe,\ Gaye,\ Maria\ Nugent,\ and\ Frances\ Morphy,\ eds\.\ 2021\.\ Ancestors,\ artefacts,\ empire:\ Indigenous\ Australia\ in\ British\ and\ Irish\ Museums\.\ London:\ The\ British\ Museum\ \&\ The\ National\ Museum\ of\ Australia\.\|The\ relevant\ entry\ is\ on\ p\.254:\|"Torquay\ Museum:\ From\ the\ Cairns\ region,\ a\ message\ stick\ from\ photographer\ and\ collector\ W\.\ Charles\ Handley\.\ "\|Are\ you\ able\ to\ provide\ catalogue\ data\ for\ this\ item\ collected\ by\ W\.\ Charles\ Handley\?\|Many\ thanks,\|Piers"\|\[Torquay\ Museum:\]\|Dear\ Piers,\|I\ am\ sorry\ about\ the\ delay\ in\ replying\.\ I\ did\ have\ a\ look\ for\ the\ message\ stick\ and\ couldn’t\ find\ any\ trace\ of\ it\ or\ any\ mention\ of\ Handley\.\ The\ only\ item\ we\ have\|mentioning\ Cairns\ is\ a\ note,\ please\ see\ record:\|\[E594\.2\]\|I\ hadn’t\ replied\ because\ I\ was\ hoping\ to\ have\ a\ look\ in\ the\ store\ to\ see\ if\ I\ could\ find\ any\ trace\ but\ I\ haven’t\ had\ a\ chance\.\ I\ have\ looked\ through\ all\ the\ paperwork\ though\ and\ can’t\ find\ anything\.\|Sorry\ I\ can’t\ be\ of\ more\ help\.\|Let\ me\ know\ if\ there\ is\ anything\ else\ you\ need\.\|Best\ Wishes,\|Clare"\|02\.04\.2026\ PK:\ Note\ that\ these\ TM_E594_1\ and\ TM_E594_2\ are\ consecutive\ accession\ numbers\.\ Thus\ it\ is\ possible\ that\ TM_E594_2\ was\ the\ wrapper\ for\ TM_E594_1\.` (separated by ` + `) | 
[Media_IDs](http://cldf.clld.org/v1.0/terms.rdf#mediaReference) | list of `string` (separated by ` `) | References [media.csv::ID](#table-mediacsv)

## <a name="table-relatedcsv"></a>Table [related.csv](./related.csv)

Groups of related items from ContributionTable.

property | value
 --- | ---
[dc:extent](http://purl.org/dc/terms/extent) | 41


### Columns

Name/Property | Datatype | Description
 --- | --- | --- 
[ID](http://cldf.clld.org/v1.0/terms.rdf#id) | `string` | Primary key
[Stick_IDs](http://cldf.clld.org/v1.0/terms.rdf#contributionReference) | list of `string` (separated by ` `) | References [contributions.csv::ID](#table-contributionscsv)
