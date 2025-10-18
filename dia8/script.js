use colegiosOficiales;

db.createCollection("ubicacion");
db.createCollection("tipoEstable");
db.createCollection("estable");
db.createCollection("correo");
db.createCollection("telefono");
db.createCollection("niveles");
db.createCollection("grados");

db.ubicacion.createIndex({ idUbica: 1 }, { unique: true });
db.tipoEstable.createIndex({ idTipoEstable: 1 }, { unique: true });
db.estable.createIndex({ idEstable: 1 }, { unique: true });
db.correo.createIndex({ idEstable: 1 }, { unique: true });
db.telefono.createIndex({ idEstable: 1 }, { unique: true });
db.niveles.createIndex({ idEstable: 1 }, { unique: true });
db.grados.createIndex({ idEstable: 1 }, { unique: true });

db.dataBruto.aggregate([
    {
        $project: {
            _id: 0,
            zona: 1,
            direccion: 1,
            idUbica: "$_id"
        }
    },
    {
        $merge: {
            into: "ubicacion",
            on: "idUbica",
            whenMatched: "merge",
            whenNotMatched: "insert"
        }
    }
]);

db.dataBruto.aggregate([
    {
        $project: {
            _id: 0,
            tipo_Establecimiento: 1,
            especialidad: 1,
            jornadas: 1,
            idTipoEstable: "$_id"
        }
    },
    {
        $merge: {
            into: "tipoEstable",
            on: "idTipoEstable",
            whenMatched: "merge",
            whenNotMatched: "insert"
        }
    }
]);

db.dataBruto.aggregate([
    {
        $project: {
            _id: 0,
            nomEstable: "$nombreestablecimiento",
            numSedes: "$numero_de_Sedes",
            nomRector: "$nombre_Rector",
            idEstable: "$_id"
        }
    },
    {
        $merge: {
            into: "estable",
            on: "idEstable",
            whenMatched: "merge",
            whenNotMatched: "insert"
        }
    }
]);



// si se intenta hacer de manera contraria genera muchos errores(no se porque es) en los cuales que salen que las llaves son duplicadas o no deja hacer cambios
db.estable.aggregate([
    {
        $lookup: {
            from: "dataBruto",
            localField: "nomEstable",
            foreignField: "nombreestablecimiento",
            as: "infoCorreo"
        }
    },
    {
        $unwind: "$infoCorreo"
    },
    {
        $project: {
            _id: "$idEstable",
            correo: "$infoCorreo.correo_Electronico",
            idEstable: "$_id"
        }
    },
    {
        $merge: {
            into: "correo",
            on: "idEstable",
            whenMatched: "merge",
            whenNotMatched: "insert"
        }
    }
]);

db.estable.aggregate([
    {
        $lookup: {
            from: "dataBruto",
            localField: "nomEstable",
            foreignField: "nombreestablecimiento",
            as: "infoTelefono"
        }
    },
    {
        $unwind: "$infoTelefono"
    },
    {
        $project: {
            _id: "$idEstable",
            correo: "$infoTelefono.telefono",
            idEstable: "$_id"
        }
    },
    {
        $merge: {
            into: "telefono",
            on: "idEstable",
            whenMatched: "merge",
            whenNotMatched: "insert"
        }
    }
]);

db.estable.aggregate([
    {
        $lookup: {
            from: "dataBruto",
            localField: "nomEstable",
            foreignField: "nombreestablecimiento",
            as: "infoNiveles"
        }
    },
    {
        $unwind: "$infoNiveles"
    },
    {
        $project: {
            _id: "$idEstable",
            correo: "$infoNiveles.niveles",
            idEstable: "$_id"
        }
    },
    {
        $merge: {
            into: "niveles",
            on: "idEstable",
            whenMatched: "merge",
            whenNotMatched: "insert"
        }
    }
]);

db.estable.aggregate([
    {
        $lookup: {
            from: "dataBruto",
            localField: "nomEstable",
            foreignField: "nombreestablecimiento",
            as: "infogrados"
        }
    },
    {
        $unwind: "$infogrados"
    },
    {
        $project: {
            _id: "$idEstable",
            correo: "$infogrados.niveles",
            idEstable: "$_id"
        }
    },
    {
        $merge: {
            into: "niveles",
            on: "idEstable",
            whenMatched: "merge",
            whenNotMatched: "insert"
        }
    }
]);


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
    }
]);