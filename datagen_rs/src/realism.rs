//! Names, addresses, emails. Deterministic in (seed, id). Names are NOT forced
//! unique -- collisions are realistic and the identity key is the IBAN. Country
//! shaping (streets, postcodes, regions, email domains) mirrors realism.py so
//! the same gate format checks pass; exact byte-parity with Python is not
//! required because the gate keys the graph on IBAN, not on the display name.

use crate::hash::splitmix64;

#[inline]
fn pick<'a>(pool: &'a [&'a str], id: u64, salt: u64) -> &'a str {
    pool[(splitmix64(id ^ salt) % pool.len() as u64) as usize]
}

// --- Currency ------------------------------------------------------------
pub fn currency_for_country(cc: &str) -> &'static str {
    match cc {
        "US" | "PA" | "KY" => "USD",
        "GB" => "GBP",
        "DE" | "FR" | "NL" | "ES" => "EUR",
        "CH" => "CHF",
        "SG" => "SGD",
        "JP" => "JPY",
        "AE" => "AED",
        "CN" => "CNY",
        "MX" => "MXN",
        "IN" => "INR",
        "CA" => "CAD",
        "AU" => "AUD",
        "HK" => "HKD",
        "KR" => "KRW",
        "BR" => "BRL",
        _ => "USD",
    }
}

// --- Names ---------------------------------------------------------------
const FIRST: &[&str] = &[
    "James",
    "Mary",
    "Robert",
    "Patricia",
    "Michael",
    "Jennifer",
    "David",
    "Linda",
    "William",
    "Elizabeth",
    "Richard",
    "Barbara",
    "Joseph",
    "Susan",
    "Thomas",
    "Jessica",
    "Charles",
    "Sarah",
    "Christopher",
    "Karen",
    "Daniel",
    "Nancy",
    "Matthew",
    "Lisa",
    "Anthony",
    "Margaret",
    "Mark",
    "Betty",
    "Donald",
    "Sandra",
    "Steven",
    "Ashley",
    "Paul",
    "Kimberly",
    "Andrew",
    "Emily",
    "Joshua",
    "Donna",
    "Kenneth",
    "Michelle",
    "Jose",
    "Maria",
    "Juan",
    "Ana",
    "Luis",
    "Carmen",
    "Carlos",
    "Rosa",
    "Miguel",
    "Elena",
    "Wei",
    "Jing",
    "Ming",
    "Li",
    "Chen",
    "Yan",
    "Hiroshi",
    "Yuki",
    "Min-jun",
    "Ji-woo",
    "Rajesh",
    "Priya",
    "Amit",
    "Neha",
    "Vikram",
    "Anjali",
    "Mohammed",
    "Fatima",
    "Ahmed",
    "Aisha",
    "Omar",
    "Layla",
    "Kwame",
    "Ama",
    "Kofi",
    "Chidi",
    "Ngozi",
    "Sekou",
    "Zainab",
];
const LAST: &[&str] = &[
    "Smith",
    "Johnson",
    "Williams",
    "Brown",
    "Jones",
    "Davis",
    "Miller",
    "Wilson",
    "Moore",
    "Taylor",
    "Anderson",
    "Thomas",
    "Jackson",
    "White",
    "Harris",
    "Martin",
    "Thompson",
    "Robinson",
    "Clark",
    "Lewis",
    "Lee",
    "Walker",
    "Hall",
    "Allen",
    "Young",
    "King",
    "Wright",
    "Scott",
    "Green",
    "Baker",
    "Adams",
    "Nelson",
    "Hill",
    "Campbell",
    "Garcia",
    "Rodriguez",
    "Martinez",
    "Hernandez",
    "Lopez",
    "Gonzalez",
    "Perez",
    "Sanchez",
    "Ramirez",
    "Torres",
    "Flores",
    "Rivera",
    "Gomez",
    "Diaz",
    "Reyes",
    "Wang",
    "Zhang",
    "Liu",
    "Chen",
    "Yang",
    "Huang",
    "Zhao",
    "Wu",
    "Sato",
    "Suzuki",
    "Kim",
    "Park",
    "Choi",
    "Patel",
    "Sharma",
    "Singh",
    "Kumar",
    "Shah",
    "Gupta",
    "Khan",
    "Okafor",
    "Adebayo",
    "Mensah",
    "Diallo",
    "Al-Saud",
    "Al-Farsi",
    "Hassan",
    "Ibrahim",
];
const CORP_HEAD: &[&str] = &[
    "Meridian",
    "Northgate",
    "Blackwell",
    "Cambrian",
    "Pinnacle",
    "Kestrel",
    "Halcyon",
    "Cornerstone",
    "Ironbridge",
    "Silverline",
    "Eastvale",
    "Redwood",
    "Arcadia",
    "Brightwater",
    "Summit",
    "Anchor",
    "Beacon",
    "Cardinal",
    "Delta",
    "Pacific",
    "Atlantic",
    "Prime",
    "Vantage",
    "Sterling",
    "Titan",
    "Vertex",
    "Zenith",
    "Coastal",
    "Highland",
    "Riverbend",
    "Stonebridge",
    "Westfield",
    "Fairview",
    "Heritage",
    "Legacy",
    "Union",
    "Alliance",
    "Continental",
    "Global",
    "Universal",
    "Strategic",
    "Dynamic",
    "Precision",
    "Advanced",
    "Integrated",
    "Unified",
    "Consolidated",
    "Federated",
    "Independent",
    "Regional",
];
const CORP_DESC: &[&str] = &[
    "Holdings",
    "Industries",
    "Trading",
    "Capital",
    "Logistics",
    "Manufacturing",
    "Services",
    "Resources",
    "Technologies",
    "Solutions",
    "Partners",
    "Group",
    "Enterprises",
    "Systems",
    "Ventures",
    "Consulting",
    "Investments",
    "Properties",
];
const FI_SUFFIX: &[&str] = &[
    "Bank",
    "International Bank",
    "Trust",
    "Financial",
    "Capital Bank",
    "National Bank",
    "Merchant Bank",
    "Private Banking",
    "Securities",
];

fn legal_suffix(cc: &str, id: u64) -> &'static str {
    let pool: &[&str] = match cc {
        "US" => &["Inc", "LLC", "Corp", "LP"],
        "GB" => &["Ltd", "plc", "LLP"],
        "DE" => &["GmbH", "AG", "KG"],
        "FR" => &["SA", "SARL", "SAS"],
        "CH" => &["AG", "GmbH", "SA"],
        "SG" => &["Pte Ltd", "Ltd"],
        "JP" => &["KK", "GK"],
        "AE" => &["LLC", "Holdings"],
        "IN" => &["Ltd", "Pvt Ltd"],
        "MX" => &["SA de CV", "S de RL"],
        "CN" => &["Ltd", "Co Ltd"],
        "CA" => &["Inc", "Ltd", "Corp"],
        "BR" => &["SA", "Ltda"],
        "AU" => &["Pty Ltd", "Ltd"],
        "HK" => &["Ltd", "HK Ltd"],
        "KR" => &["Ltd", "Corp"],
        "NL" => &["BV", "NV"],
        "ES" => &["SA", "SL"],
        _ => &["Ltd"],
    };
    pool[(splitmix64(id ^ 0x11E6A1) % pool.len() as u64) as usize]
}

pub fn person_name(id: u64, seed: i64) -> String {
    let f = pick(FIRST, id, (seed as u64).wrapping_add(121));
    let l = pick(LAST, id, (seed as u64).wrapping_add(131));
    let mi = (b'A' + (splitmix64(id ^ (seed as u64).wrapping_add(141)) % 26) as u8) as char;
    format!("{} {}. {}", f, mi, l)
}

pub fn company_name(id: u64, cc: &str, seed: i64) -> String {
    let h = pick(CORP_HEAD, id, (seed as u64).wrapping_add(141));
    let d = pick(CORP_DESC, id, (seed as u64).wrapping_add(151));
    format!("{} {} {}", h, d, legal_suffix(cc, id))
}

pub fn fi_name(id: u64, seed: i64) -> String {
    let h = pick(CORP_HEAD, id, (seed as u64).wrapping_add(171));
    let s = pick(FI_SUFFIX, id, (seed as u64).wrapping_add(181));
    format!("{} {}", h, s)
}

// --- Addresses -----------------------------------------------------------
const US_STREETS: &[&str] = &[
    "Main Street",
    "Broadway",
    "Park Avenue",
    "Oak Avenue",
    "Maple Drive",
    "Pine Road",
    "Cedar Lane",
    "Elm Street",
    "Washington Street",
    "Lincoln Avenue",
    "Jefferson Boulevard",
    "Madison Avenue",
    "Franklin Street",
    "Church Street",
    "River Road",
    "Lakeview Drive",
    "Hillcrest Drive",
    "Highland Avenue",
    "Sunset Boulevard",
    "2nd Street",
    "5th Avenue",
    "Market Street",
    "Union Street",
    "Spring Street",
    "Ridge Road",
];
const GB_STREETS: &[&str] = &[
    "High Street",
    "Station Road",
    "Church Lane",
    "Victoria Road",
    "Kings Road",
    "Queens Road",
    "Mill Lane",
    "Bridge Street",
    "Church Street",
    "London Road",
    "New Road",
    "Park Road",
    "Manor Road",
    "School Lane",
    "The Green",
    "Queens Gardens",
    "Wellington Terrace",
    "Cathedral Close",
    "Commercial Way",
    "Market Square",
];
const DE_STREETS: &[&str] = &[
    "Hauptstrasse",
    "Bahnhofstrasse",
    "Schulstrasse",
    "Gartenstrasse",
    "Dorfstrasse",
    "Kirchstrasse",
    "Bergstrasse",
    "Lindenstrasse",
    "Goethestrasse",
    "Schillerstrasse",
    "Marktplatz",
    "Ringstrasse",
    "Waldweg",
    "Wiesenweg",
    "Am Bach",
];
const FR_STREETS: &[&str] = &[
    "Rue de la Paix",
    "Rue Victor Hugo",
    "Avenue des Champs",
    "Rue de la Gare",
    "Rue Nationale",
    "Rue de l'Eglise",
    "Boulevard Saint-Michel",
    "Rue du Moulin",
    "Place de la Republique",
    "Rue Jean Jaures",
    "Avenue de la Liberte",
    "Rue des Ecoles",
    "Rue de la Mairie",
    "Chemin des Vignes",
    "Rue Pasteur",
];
const INTL_STREETS: &[&str] = &[
    "Main Street",
    "Market Square",
    "Station Road",
    "Park Avenue",
    "Central Avenue",
    "Commerce Street",
    "Harbour Road",
    "Garden Street",
    "River Road",
    "Union Street",
    "Bank Street",
    "Cathedral Square",
    "Liberty Avenue",
    "Victory Boulevard",
    "Industrial Road",
];

fn streets_for(cc: &str) -> (&'static [&'static str], bool) {
    // (pool, number_after)
    match cc {
        "US" | "CA" | "AU" => (US_STREETS, false),
        "GB" => (GB_STREETS, false),
        "DE" | "AT" | "CH" => (DE_STREETS, true),
        "FR" | "BE" => (FR_STREETS, false),
        _ => (INTL_STREETS, false),
    }
}

pub fn street(id: u64, cc: &str, seed: i64) -> String {
    let (pool, after) = streets_for(cc);
    let s = pool[(splitmix64(id ^ (seed as u64).wrapping_add(211)) % pool.len() as u64) as usize];
    let num = splitmix64(id ^ (seed as u64).wrapping_add(201)) % 9998 + 1;
    if after {
        format!("{} {}", s, num)
    } else {
        format!("{} {}", num, s)
    }
}

const CITIES_US: &[&str] = &[
    "New York",
    "Los Angeles",
    "Chicago",
    "Houston",
    "Phoenix",
    "Philadelphia",
    "San Antonio",
    "San Diego",
    "Dallas",
    "San Jose",
    "Austin",
    "Jacksonville",
    "Fort Worth",
    "Columbus",
    "Charlotte",
    "Indianapolis",
    "San Francisco",
    "Seattle",
    "Denver",
    "Boston",
];
const CITIES_GB: &[&str] = &[
    "London",
    "Birmingham",
    "Manchester",
    "Leeds",
    "Glasgow",
    "Liverpool",
    "Bristol",
    "Edinburgh",
    "Cardiff",
    "Belfast",
    "Nottingham",
    "Southampton",
    "Reading",
    "Oxford",
];
const CITIES_INTL: &[&str] = &[
    "Central",
    "Riverside",
    "Lakeside",
    "Portside",
    "Midtown",
    "Uptown",
    "Eastgate",
    "Westhaven",
    "Northfield",
    "Southport",
    "Greenwood",
    "Fairmont",
];
fn cities_for(cc: &str) -> &'static [&'static str] {
    match cc {
        "US" | "CA" | "AU" => CITIES_US,
        "GB" => CITIES_GB,
        _ => CITIES_INTL,
    }
}
pub fn city(id: u64, cc: &str, seed: i64) -> String {
    let pool = cities_for(cc);
    pool[(splitmix64(id ^ (seed as u64).wrapping_add(191)) % pool.len() as u64) as usize]
        .to_string()
}

fn regions_for(cc: &str) -> &'static [&'static str] {
    match cc {
        "US" => &[
            "CA", "TX", "NY", "FL", "IL", "PA", "OH", "GA", "NC", "MI", "NJ", "VA", "WA", "AZ",
            "MA", "TN", "IN", "MO", "MD", "CO",
        ],
        "CA" => &["ON", "QC", "BC", "AB", "MB", "SK", "NS", "NB", "NL", "PE"],
        "AU" => &["NSW", "VIC", "QLD", "WA", "SA", "TAS", "ACT", "NT"],
        "GB" => &[
            "England",
            "Scotland",
            "Wales",
            "Northern Ireland",
            "Greater London",
            "West Midlands",
        ],
        "DE" => &["Bayern", "Hessen", "Berlin", "Sachsen", "Niedersachsen"],
        "FR" => &[
            "Ile-de-France",
            "Occitanie",
            "Bretagne",
            "Grand Est",
            "Normandie",
        ],
        "ES" => &["Madrid", "Cataluna", "Andalucia", "Valencia", "Galicia"],
        "IN" => &["Maharashtra", "Delhi", "Karnataka", "Tamil Nadu", "Gujarat"],
        "MX" => &["CDMX", "Jalisco", "Nuevo Leon", "Puebla", "Mexico"],
        "BR" => &[
            "Sao Paulo",
            "Rio de Janeiro",
            "Minas Gerais",
            "Bahia",
            "Parana",
        ],
        "CN" => &["Guangdong", "Shandong", "Jiangsu", "Zhejiang", "Sichuan"],
        "JP" => &["Tokyo", "Osaka", "Kanagawa", "Aichi", "Hokkaido"],
        "KR" => &["Seoul", "Gyeonggi", "Busan", "Incheon", "Daegu"],
        "NL" => &[
            "Noord-Holland",
            "Zuid-Holland",
            "Utrecht",
            "Gelderland",
            "Limburg",
        ],
        "CH" => &["Zurich", "Bern", "Geneve", "Vaud", "Aargau"],
        "SG" => &[
            "Central",
            "North East",
            "North West",
            "South East",
            "South West",
        ],
        "AE" => &["Dubai", "Abu Dhabi", "Sharjah", "Ajman", "Fujairah"],
        "HK" => &["Hong Kong Island", "Kowloon", "New Territories"],
        _ => &["Central", "North", "South", "East", "West"],
    }
}
pub fn region(id: u64, cc: &str, seed: i64) -> String {
    let pool = regions_for(cc);
    pool[(splitmix64(id ^ (seed as u64).wrapping_add(313)) % pool.len() as u64) as usize]
        .to_string()
}

const PCL: &[u8; 22] = b"ABCDEFGHJKLMNPRSTUWXYZ";
pub fn postcode(id: u64, cc: &str, seed: i64) -> String {
    let v = splitmix64(id ^ (seed as u64).wrapping_add(999));
    let w = splitmix64(id ^ (seed as u64).wrapping_add(997));
    let l = |x: u64| PCL[(x % 22) as usize] as char;
    match cc {
        "US" | "DE" | "FR" | "ES" | "MX" | "KR" => format!("{:05}", v % 89999 + 10000),
        "CN" | "SG" | "IN" => format!("{:06}", v % 899999 + 100000),
        "JP" => format!("{:03}-{:04}", v % 900 + 100, w % 9000 + 1000),
        "AU" | "CH" => format!("{:04}", v % 8999 + 1000),
        "BR" => format!("{:05}-{:03}", v % 89999 + 10000, w % 900 + 100),
        "NL" => format!("{:04} {}{}", v % 9000 + 1000, l(v), l(w)),
        "GB" => format!(
            "{}{}{} {}{}{}",
            l(v),
            l(w),
            v % 9 + 1,
            w % 9 + 1,
            l(v >> 4),
            l(w >> 4)
        ),
        "CA" => format!(
            "{}{}{} {}{}{}",
            l(v),
            w % 10,
            l(v >> 4),
            w % 10,
            l(w >> 4),
            v % 10
        ),
        "AE" => format!("PO Box {}", v % 89999 + 10000),
        "HK" => "000000".to_string(),
        _ => format!("{:05}", v % 89999 + 10000),
    }
}

// --- Email ---------------------------------------------------------------
const GLOBAL_DOMAINS: &[&str] = &[
    "gmail.com",
    "yahoo.com",
    "hotmail.com",
    "outlook.com",
    "icloud.com",
    "proton.me",
];
fn local_domains(cc: &str) -> &'static [&'static str] {
    match cc {
        "US" => &["aol.com", "comcast.net"],
        "GB" => &["btinternet.com", "sky.com"],
        "DE" => &["gmx.de", "web.de", "t-online.de"],
        "FR" => &["orange.fr", "free.fr", "laposte.net"],
        "ES" => &["telefonica.net"],
        "JP" => &["yahoo.co.jp", "docomo.ne.jp"],
        "CN" => &["qq.com", "163.com"],
        "IN" => &["rediffmail.com"],
        "BR" => &["uol.com.br"],
        "NL" => &["ziggo.nl"],
        "CA" => &["rogers.com"],
        "AU" => &["bigpond.com"],
        _ => &[],
    }
}
const LEGAL_TOKENS: &[&str] = &[
    "inc", "llc", "corp", "lp", "ltd", "plc", "llp", "gmbh", "ag", "kg", "sa", "sarl", "sas",
    "pte", "kk", "gk", "bv", "nv", "pvt", "co", "sl", "ltda", "pty", "de", "cv", "rl", "s",
];

/// Alphabetic local part: drop parentheticals, legal suffixes, middle initials.
pub fn email_local(name: &str) -> String {
    let cleaned: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphabetic() {
                c.to_ascii_lowercase()
            } else {
                ' '
            }
        })
        .collect();
    let tokens: Vec<&str> = cleaned
        .split_whitespace()
        .filter(|t| t.len() > 1 && !LEGAL_TOKENS.contains(t))
        .collect();
    if tokens.is_empty() {
        "user".to_string()
    } else if tokens.len() >= 2 {
        format!("{}.{}", tokens[0], tokens[tokens.len() - 1])
    } else {
        tokens[0].to_string()
    }
}

pub fn phone(id: u64, cc: &str, seed: i64) -> String {
    let prefix = match cc {
        "US" | "CA" => "+1",
        "GB" => "+44",
        "DE" => "+49",
        "FR" => "+33",
        "JP" => "+81",
        "CN" => "+86",
        "IN" => "+91",
        "SG" => "+65",
        "CH" => "+41",
        "AE" => "+971",
        "MX" => "+52",
        "BR" => "+55",
        "AU" => "+61",
        "HK" => "+852",
        "KR" => "+82",
        "NL" => "+31",
        "ES" => "+34",
        _ => "+1",
    };
    let r = splitmix64(id ^ (seed as u64).wrapping_add(221));
    let r2 = splitmix64(id ^ (seed as u64).wrapping_add(231));
    format!(
        "{} {}-{}-{:04}",
        prefix,
        r % 900 + 100,
        r2 % 900 + 100,
        r / 1000 % 10000
    )
}

pub fn email(name: &str, id: u64, cc: &str, seed: i64) -> String {
    let locals = local_domains(cc);
    let n = GLOBAL_DOMAINS.len() + locals.len();
    let di = (splitmix64(id ^ (seed as u64).wrapping_add(241)) % n as u64) as usize;
    let domain = if di < GLOBAL_DOMAINS.len() {
        GLOBAL_DOMAINS[di]
    } else {
        locals[di - GLOBAL_DOMAINS.len()]
    };
    format!("{}{:02}@{}", email_local(name), id % 100, domain)
}
