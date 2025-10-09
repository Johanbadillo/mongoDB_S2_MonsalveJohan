use incautacionesPruebas;

// ¿Cuántos municipios comienzan con "La" y cuál es la cantidad total incautada en ellos?

/*
db.evento.aggregate([
    {
        $match: {
            municipios: { regex: "^(la)", $options: "i" }
        }
    },
    {
        $group: {
            _id: "$cod_muni",
            cantidad: {$sum: "$cantidad"}
        }
    }
]);
*/


db.evento.aggregate([
    {
        $lookup: {
            from: "municipios",
            localField: "cod_muni",
            foreignField: "codMuni",
            as: "municipioInfo"
        }
    },
    {
        $unwind: "$municipioInfo"
    },
    {
        $match: {
            "municipioInfo.nomMuni": { $regex: /^puerto/i }
        }
    },
    {
        $group: {
            _id: "$municipioInfo.codMuni",
            nomMuni: { $first: "$municipioInfo.nomMuni" },
            cantidadTotal: { $sum: "$cantidad" }
        }
    }
]);

// Top 5 departamentos donde los municipios terminan en "al" y la cantidad incautada.

db.evento.aggregate([
    {
        $lookup: {
            from: "municipios",
            localField: "cod_muni",
            foreignField: "codMuni",
            as: "municipioInfo"
        }
    },
    {
        $match: {
            "municipioInfo.nomMuni": { $regex: "al$", $options: "i" }
        }
    },
    {
        $group: {
            _id: "$municipioInfo.codMuni",
            nomMuni: { $first: "$municipioInfo.nomMuni" },
            cantidadTotal: { $sum: "$cantidad" }
        }
    },
    {
        $sort: { cantidadTotal: -1 }
    },
    {
        $limit: 5
    }
]);

// Por cada año, muestra los 3 municipios con más incautaciones, pero únicamente si su nombre contiene la letra "z".


// Esta incompleto falta que solo sea 3 por año
db.evento.aggregate([
    {
        $lookup: {
            from: "municipios",
            localField: "cod_muni",
            foreignField: "codMuni",
            as: "municipioInfo"
        }
    },
    {
        $match: {
            "municipioInfo.nomMuni": { $regex: "z", $options: "i" }
        }
    },
    {
        $addFields: {
            anio: { $year: '$fechaHecho' }
        }
    },
    {
        $group: {
            _id: {
                anio: "$anio",
                codMuni: "$municipioInfo.codMuni"
            },
            nomMuni: { $first: "$municipioInfo.nomMuni" },
            cantidadTotal: { $sum: "$cantidad" }
        }
    },
    {
        $sort: {
            "_id.anio": 1,
            cantidadTotal: -1
        }
    }
]);

//  ¿Qué unidad de medida aparece en registros de municipios que empiecen por "Santa"?

// Arreglar la colecion evento Porque no existe la el id para hacer una relacion para el lookup de unidades

db.evento.aggregate([
    {
        $lookup: {
            from: "municipios",
            localField: "cod_muni",
            foreignField: "codMuni",
            as: "municipioInfo"
        }
    },
    {
        $match: {
            "municipioInfo.nomMuni": { $regex: "^(santa)", $options: "i" }
        }
    },
    {
        $lookup: {
            from: "unidades",
            localField: "unidadUsada",
            foreignField: "nomUnidad",
            as: "unidadInfo"
        }
    },
    {
        $group: {
            _id: "$municipioInfo.codMuni",
            nomMuni: { $first: "$municipioInfo.nomMuni" },
            /*UnidadUsada: {$first: "$unidadInfo.nomUnidad"}*/
        }
    }
]);

// ¿Cuál es la cantidad promedio de incautaciones en los municipios cuyo nombre contiene "Valle"?

// El lookup se consume muchisimo entonces es mejor dejarlo casi al final porque hace que se procese toda la data que trae
db.evento.aggregate([
    {
        $lookup: {
            from: "municipios",
            localField: "cod_muni",
            foreignField: "codMuni",
            as: "municipioInfo"
        }
    },
    {
        $match: {
            "municipioInfo.nomMuni": { $regex: "(valle)", $options: "i" }
        }
    },
    {
        $group: {
            _id: "$municipioInfo.codMuni",
            nomMuni: { $first: "$municipioInfo.nomMuni" },
            cantidadTotal: { $avg: "$cantidad" }
        }
    },
    {
        $sort: {
            cantidadTotal: 1
        }
    }
]);

// ¿Cuántos registros hay en municipios cuyo nombre contenga exactamente 7 letras?

db.municipios.aggregate([
    {
        $match: {
            nomMuni: { $regex: "^.{7}$" }
        }
    },
    {
        $count: "totalMunicipios"
    }
]);



// ¿Cuáles son los 5 años con menor cantidad incautada en todo el país?

db.evento.aggregate([
    {
        $addFields: {
            anio: { $year: '$fechaHecho' }
        }
    },
    {
        $group: {
            _id: { anio: "$anio" },
            cantidadTotal: { $sum: "$cantidad" }
        }
    },
    {
        $sort: {
            cantidadTotal: 1
        }
    },
    {
        $limit: 5
    }
]);


// Muestra la cantidad total incautada por unidad de medida, ordenada de mayor a menor.
// no se puede realizar porque no se hizo la relacion del eveto con la unidad


// Identifica los municipios que nunca superaron 1 kilogramo de incautación en todos sus registros.

db.evento.aggregate([
    {
        $lookup: {
            from: "municipios",
            localField: "cod_muni",
            foreignField: "codMuni",
            as: "municipioInfo"
        }
    },
    {
        $group: {
            _id: "$municipioInfo.codMuni",
            nomMuni: { $first: "$municipioInfo.nomMuni" },
            cantidadTotal: { $sum: "$cantidad" }
        }
    }, {
        $match: {
            cantidadTotal: { $lt: 1 }
        }
    }
]);

// Encuentra los municipios cuyo nombre empieza por "Puerto" y determina el total incautado en cada año.

db.evento.aggregate([
    {
        $lookup: {
            from: "municipios",
            localField: "cod_muni",
            foreignField: "codMuni",
            as: "municipioInfo"
        }
    },
    {
        $match: {
            "municipioInfo.nomMuni": { $regex: "^(puerto)", $options: "i" }
        }
    },
    {
        $addFields: {
            anio: { $year: '$fechaHecho' }
        }
    },
    {
        $group: {
            _id: { anio: "$anio", nomMuni: "$municipioInfo.nomMuni" },
            cantidadTotal: { $sum: "$cantidad" }
        }
    },
    {
        $sort: {
            "_id.anio": 1,
            "_id.nomMuni": 1
        }
    }
]);


db.evento.aggregate([
    {
        $group:{
            _id: "$cod_muni",
            promedio: {$avg: "$cantidad"}
        }
    },
    {
        $sort:{
            promedio: -1
        }
    },
    {
        $limit: 10
    },
    {
        $lookup: {
            from: "municipios",
            localField: "_id",
            foreignField: "codMuni",
            as: "municipioInfo"
        }
    },
    {
        $unwind: "$municipioInfo"
    },
    {
        $project:{
            nomMuni: "$municipioInfo.nomMuni",
            promedio: 1
        }
    }
]);