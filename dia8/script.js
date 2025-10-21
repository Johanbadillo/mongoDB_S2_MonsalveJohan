use  colegiosOficiales;

db.createCollection("telefono");
db.createCollection("niveles");
db.createCollection("jornadas");
db.createCollection("especialidades");
db.createCollection("grados");
db.createCollection("correo");

db.createCollection("Estable");
db.createCollection("ubi");
db.createCollection("tipoEstable");

db.telefono.drop();
db.niveles.drop();
db.jornadas.drop();
db.especialidades.drop();
db.grados.drop();
db.correo.drop();

db.tipoEstable.drop();
db.estable.drop();
db.ubi.drop();



// TELEFONO
db.dataBruto.aggregate([
    {
        $project: {
            _id: 1,
            telefono: { $split: [ {$trim: { input: "$telefono", chars: " "}}, "--"] }
        }
    },
    {
        $merge: {
            into: "telefonoPrueba",
            on: "_id",
            whenMatched: "merge",
            whenNotMatched: "insert"
        }
    }
]);

db.telefonoPrueba.aggregate([
    {
        $unwind: "$telefono"
    },
    {
        $project: {
            _id: 1,
            telefono: { $split: [ {$trim: { input: "$telefono", chars: " "}}, "/"] }
        }
    },
    {
        $unwind: "$telefono"
    },
    {
        $group: {
            _id: "$_id",
            telefono: {
                $push: {
                    telefono: "$telefono"
                }
            }
        }
    },
    {
        $merge: {
            into: "telefono",
            on: "_id",
            whenMatched: "merge",
            whenNotMatched: "insert"
        }
    }
]);

// eliminar luego de su uso ya que no tiene algun uso extra
db.telefonoPrueba.drop();
db.telefono.drop();

//---------------------------------------------------------------
// Niveles

db.dataBruto.aggregate([
    {
        $project: {
            _id: 1,
            niveles: { $split: ["$niveles", ","] }
        }
    },
    {
        $unwind: "$niveles"
    },
    {
        $project: {
            _id: "$_id",
            niveles:  "$niveles"
        }
    },
    {
        $group: {
            _id: "$_id",
            niveles: {
                $push: {
                    niveles: "$niveles"
                }
            }
        }
    },
    {
        $merge: {
            into: "niveles",
            on: "_id",
            whenMatched: "merge",
            whenNotMatched: "insert"
        }
    }
]);


// eliminar luego de su uso ya que no tiene algun uso extra
db.niveles.drop();

//---------------------------------------------------------------
// Jornadas

db.dataBruto.aggregate([
    {
        $project: {
            _id: 1,
            jornadas: { $split: ["$jornadas", ","] }
        }
    },
    {
        $unwind: "$jornadas"
    },
    {
        $project: {
            _id: "$_id",
            jornadas:  "$jornadas"
        }
    },
    {
        $group: {
            _id: "$_id",
            jornadas: {
                $push: {
                    jornadas: "$jornadas"
                }
            }
        }
    },
    {
        $merge: {
            into: "jornadas",
            on: "_id",
            whenMatched: "merge",
            whenNotMatched: "insert"
        }
    }
]);

// eliminar luego de su uso ya que no tiene algun uso extra
db.jornadas.drop();

//---------------------------------------------------------------
// Especialidad

db.dataBruto.aggregate([
    {
        $project: {
            _id: 1,
            especialidad: { $split: ["$especialidad", ","] }
        }
    },
    {
        $unwind: "$especialidad"
    },
    {
        $project: {
            _id: "$_id",
            especialidad:  "$especialidad"
        }
    },
    {
        $group: {
            _id: "$_id",
            especialidades: {
                $push: {
                    especialidad: "$especialidad"
                }
            }
        }
    },
    {
        $merge: {
            into: "especialidades",
            on: "_id",
            whenMatched: "merge",
            whenNotMatched: "insert"
        }
    }
]);

// eliminar luego de su uso ya que no tiene algun uso extra
db.especialidades.drop();

//---------------------------------------------------------------
// Grados

db.dataBruto.aggregate([
    {
        $project: {
            _id: 1,
            grados: { $split: ["$grados", ","] }
        }
    },
    {
        $unwind: "$grados"
    },
    {
        $project: {
            _id: "$_id",
            grado: { $toInt: "$grados" }
        }
    },
    {
        $sort: {grado: 1}
    },
    {
        $match: {
            grado: { $gte: 0, $lte: 11 }
        }
    },
    {
        $group: {
            _id: "$_id",
            grados: {
                $push: {
                    grado: "$grado"
                }
            }
        }
    },
    {
        $merge: {
            into: "grados",
            on: "_id",
            whenMatched: "merge",
            whenNotMatched: "insert"
        }
    }
]);

// eliminar luego de su uso ya que no tiene algun uso extra
db.grados.drop();

//---------------------------------------------------------------
// Correo

db.dataBruto.aggregate([
    {
        $project: {
            _id: 1,
            correo: { $split: [ {$trim: { input: "$correo_Electronico", chars: " "}}, "--"] }
        }
    },
    {
        $merge: {
            into: "correoPrueba",
            on: "_id",
            whenMatched: "merge",
            whenNotMatched: "insert"
        }
    }
]);

db.correoPrueba.aggregate([
    {
        $unwind: "$correo"
    },
    {
        $project: {
            _id: 1,
            correo: { $split: [ {$trim: { input: "$correo", chars: " "}}, "-"] }
        }
    },
    {
        $unwind: "$correo"
    },
    {
        $group: {
            _id: "$_id",
            correo: {
                $push: {
                    correo: "$correo"
                }
            }
        }
    },
    {
        $merge: {
            into: "correo",
            on: "_id",
            whenMatched: "merge",
            whenNotMatched: "insert"
        }
    }
]);

db.correo.drop();

//---------------------------------------------------------------
// Estable

db.dataBruto.aggregate([
    {
        $project: {
            _id: "$_id",
            nomEstable: "$nombreestablecimiento",
            nomRector: "$nombre_Rector",
            numSedes: "$numero_de_Sedes"
        }
    },
    {
        $merge: {
            into: "estable",
            on: "_id",
            whenMatched: "merge",
            whenNotMatched: "insert"
        }
    }
]);

//---------------------------------------------------------------
// ubicacion


db.dataBruto.aggregate([
    {
        $project: {
            _id: "$_id",
            dirrecion: "$direccion",
            zona: "$zona"
        }
    },
    {
        $merge: {
            into: "ubi",
            on: "_id",
            whenMatched: "merge",
            whenNotMatched: "insert"
        }
    }
]);



//---------------------------------------------------------------
// Tipo Establecimiento

db.dataBruto.aggregate([
    {
        $project: {
            _id: 1,
            tipoEstable: "$tipo_Establecimiento"
        }
    },
    {
        $merge: {
            into: "tipoEstable",
            on: "_id",
            whenMatched: "merge",
            whenNotMatched: "insert"
        }
    }
]);



db.estable.aggregate([
    {
        $lookup: {
            from: "correo",
            localField: "_id",
            foreignField: "_id",
            as: "correo"
        }
    },
    {
        $unwind: "$correo"
    },
    {
        $project: {
            _id: 1,
            nomEstable: 1,
            nomRector: 1,
            numSedes: 1,
            correos: "$correo.correo"
        }
    },
    {
        $merge: {
            into: "estable",
            on: "_id",
            whenMatched: "merge",
            whenNotMatched: "insert"
        }
    }
]);

db.estable.aggregate([
    {
        $lookup: {
            from: "telefono",
            localField: "_id",
            foreignField: "_id",
            as: "telefono"
        }
    },
    {
        $unwind: "$telefono"
    },
    {
        $project: {
            _id: 1,
            nomEstable: 1,
            nomRector: 1,
            numSedes: 1,
            telefonos: "$telefono.telefono"
        }
    },
    {
        $merge: {
            into: "estable",
            on: "_id",
            whenMatched: "merge",
            whenNotMatched: "insert"
        }
    }
]);

db.estable.aggregate([
    {
        $lookup: {
            from: "grados",
            localField: "_id",
            foreignField: "_id",
            as: "grados"
        }
    },
    {
        $unwind: "$grados"
    },
    {
        $project: {
            _id: 1,
            nomEstable: 1,
            nomRector: 1,
            numSedes: 1,
            grados: "$grados.grados"
        }
    },
    {
        $merge: {
            into: "estable",
            on: "_id",
            whenMatched: "merge",
            whenNotMatched: "insert"
        }
    }
]);


db.estable.aggregate([
    {
        $lookup: {
            from: "niveles",
            localField: "_id",
            foreignField: "_id",
            as: "niveles"
        }
    },
    {
        $unwind: "$niveles"
    },
    {
        $project: {
            _id: 1,
            nomEstable: 1,
            nomRector: 1,
            numSedes: 1,
            niveles: "$niveles.niveles"
        }
    },
    {
        $merge: {
            into: "estable",
            on: "_id",
            whenMatched: "merge",
            whenNotMatched: "insert"
        }
    }
]);

db.estable1.aggregate([
    {
        $lookup: {
            from: "correo",
            localField: "_id",
            foreignField: "_id",
            as: "correo"
        }
    },
    {
        $unwind: "$correo"
    },
    {
        $lookup: {
            from: "telefono",
            localField: "_id",
            foreignField: "_id",
            as: "telefono"
        }
    },
    {
        $unwind: "$telefono"
    },
    {
        $lookup: {
            from: "niveles",
            localField: "_id",
            foreignField: "_id",
            as: "niveles"
        }
    },
    {
        $unwind: "$niveles"
    },
    {
        $lookup: {
            from: "grados",
            localField: "_id",
            foreignField: "_id",
            as: "grados"
        }
    },
    {
        $unwind: "$grados"
    },
    {
        $project: {
            _id: 1,
            nomEstable: 1,
            nomRector: 1,
            numSedes: 1,
            niveles: "$niveles.niveles",
            correos: "$correo.correo",
            telefonos: "$telefono.telefono",
            grados: "$grados.grado",
        }
    }
     
]);